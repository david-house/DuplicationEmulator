using Microsoft.Data.SqlClient;
using System.Data;
using System.Threading.Tasks.Dataflow;

namespace DuplicationEmulator;


internal class Program
{
    readonly static string TableDDL = """
        CREATE TABLE Simulacrum (
        ROWID INT IDENTITY(1,1) PRIMARY KEY,
        PREFIX CHAR(3) NOT NULL,
        SUFFIX CHAR(5) NOT NULL,
        TAKEN_DATE DATETIMEOFFSET NULL,
        INDEX UQ_PREFIX_SUFFIX UNIQUE (PREFIX, SUFFIX)
        );
        """;

    static async Task Main(string[] args)
    {
        bool load = false;

        if (load)
        {
            var cnStr = "SERVER=nixnas;DATABASE=target;USER ID=sa;PASSWORD=Cession01@@@;ENCRYPT=No;";
            using var cn = new SqlConnection(cnStr);
            using var cmd = new SqlCommand("", cn);
            char[] alpha = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".ToCharArray();
            List<string> insertScripts = new List<string>();
            var pageSize = 10000;
            var suffixSize = 100000;
            cn.Open();
            for (int i = 0; i < alpha.Length; i++)
            {
                for (int j = 1; j <= suffixSize; j++)
                {
                    // spill every 10k rows
                    if (j % pageSize == 0)
                    {
                        cmd.CommandText = string.Join(Environment.NewLine, insertScripts);
                        cmd.ExecuteNonQuery();
                        Console.WriteLine($"Inserted {j} rows for {alpha[i]}");
                        insertScripts.Clear();

                        if (j == suffixSize)
                        {
                            continue;
                        }
                    }

                    var sql = $"INSERT INTO Simulacrum (Prefix, Suffix) VALUES ('{alpha[i]}01', {j:D5});";
                    insertScripts.Add(sql);

                }
            }
            cn.Close();

        }
        DataflowBlockOptions startOptions = new DataflowBlockOptions()
        { 
            BoundedCapacity = -1,
        };

        ExecutionDataflowBlockOptions executionOptions = new ExecutionDataflowBlockOptions()
        {
            MaxDegreeOfParallelism = 25,
        };

        var startBlock = new BufferBlock<ResultPayload>();
        var getNextBlock = new TransformBlock<ResultPayload, ResultPayload>(Transforms.GetNextSmallest, executionOptions);
        var saveBuffer = new BufferBlock<ResultPayload>(startOptions);

        var linkOptions = new DataflowLinkOptions { PropagateCompletion = false };
        startBlock.LinkTo(getNextBlock, linkOptions);

        var saveBlock = new ActionBlock<ResultPayload>(Transforms.SaveRecord, new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = 1 });
        getNextBlock.LinkTo(saveBuffer, linkOptions);
        saveBuffer.LinkTo(saveBlock, linkOptions);

        for (int i = 0; i < 1000; i++)
        {
            startBlock.Post(new ResultPayload(i));
            
        }


        while (startBlock.Count > 0 || 
            getNextBlock.InputCount > 0 || 
            saveBuffer.Count > 0 ||
            saveBlock.InputCount > 0)
        {
            await Task.Delay(1000);
            Console.WriteLine($"{startBlock.Count} {getNextBlock.InputCount} {getNextBlock.OutputCount} {saveBuffer.Count} {saveBlock.InputCount}");

        }
        Task.Delay(1000).Wait();
        Console.WriteLine(ResultPayloadAnalyzer.GetResults());
    }

    
}

public class ResultPayload
{
    public int Id { get; set; }
    public DateTimeOffset? EnqueueTime { get; set; } = DateTimeOffset.Now;
    public DateTimeOffset? SqlStartTime { get; set; }
    public DateTimeOffset? SqlEndTime { get; set; }
    public TimeSpan? SqlDuration => SqlStartTime.HasValue && SqlEndTime.HasValue ? SqlEndTime - SqlStartTime : null;
    public string? Prefix { get; set; }
    public string? Suffix { get; set; }
    public string Composite => $"{Prefix} {Suffix}";
    public bool IsDeadlockVictim { get; set; } = false;
    public int Spid { get; set; }
    public int DurationMs { get; set; }

    public ResultPayload() { }
    public ResultPayload(int i)
    {
        Id = i;
    }
    public string GetDisplayString()
    {
        return $"{Id}\t{SqlStartTime:o}\t{Prefix}\t{Suffix}\t{Composite}\t{SqlDuration?.TotalMilliseconds:0}\t{Spid}\t{IsDeadlockVictim}{Environment.NewLine}";
    }
}

public static class Transforms
{
    public static void SaveRecord(ResultPayload payload)
    {

        File.AppendAllText("results.txt", payload.GetDisplayString());
    }   
    public static ResultPayload GetNextSmallest(ResultPayload payload)
    {
        var sql = """
              UPDATE a
              SET TAKEN_DATE = GETDATE()
              OUTPUT INSERTED.ROWID, INSERTED.PREFIX, INSERTED.SUFFIX, INSERTED.TAKEN_DATE, @@SPID AS SPID
              FROM Simulacrum a
              WHERE a.ROWID = (SELECT MIN(b.ROWID) FROM Simulacrum b WHERE b.TAKEN_DATE IS NULL)
              AND a.TAKEN_DATE IS NULL
              """;

        var cn = new SqlConnection("SERVER=nixnas;DATABASE=target;USER ID=sa;PASSWORD=Cession01@@@;ENCRYPT=No;");
        var cmd = new SqlCommand(sql, cn);
        payload.SqlStartTime = DateTimeOffset.Now;
        cn.Open();
        using var trx = cn.BeginTransaction(IsolationLevel.ReadCommitted);
        cmd.Transaction = trx;
        try
        {
            using var reader = cmd.ExecuteReader();

            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    payload.Prefix = reader.GetString(1);
                    payload.Suffix = reader.GetString(2);
                    payload.Spid = reader.GetInt16(4);

                }
                reader.Close();
                trx.Commit();
            }
            else
            {
                payload.Prefix = "No";
                payload.Suffix = "Rows";
            }
        }
        catch (SqlException ex)
        {
            if (ex.Number == 1205)
            {
                payload.IsDeadlockVictim = true;
            }
        }

        cn.Close();
        payload.SqlEndTime = DateTimeOffset.Now;
        return payload;
    }

}

public static class ResultPayloadAnalyzer
{


    public static string GetResults()
    {
        var rps = GetResultPayloads()
            .ToArray();

        var samples = rps.Length;
        var deadlocks = rps.Count(r => r.IsDeadlockVictim);
        var avgDurationMs = rps.Average(r => r.DurationMs);
        var medianDurationMs = rps.OrderBy(x => x.DurationMs).Skip(samples / 2).First();
        var duplicates = rps.GroupBy(r => r.Composite).Where(g => g.Count() > 1).Select(g => g.Key).ToArray().Length;

        return $"n={samples} deadlocks={deadlocks} avgDurationMs={avgDurationMs:0} medianDurationMs={medianDurationMs.DurationMs:0} duplicates={duplicates}";
    }
    public static IEnumerable<ResultPayload> GetResultPayloads()
    {
        var lines = File.ReadAllLines("results.txt");
        foreach (var line in lines)
        {
            var parts = line.Split('\t');

            if (parts.Length < 8) continue;

            var id = int.Parse(parts[0]);
            var sqlStartTime = DateTimeOffset.Parse(parts[1]);
            var prefix = parts[2];
            var suffix = parts[3];
            var composite = parts[4];
            var sqlDuration = int.Parse(parts[5]);
            var spid = int.Parse(parts[6]);
            var isDeadlockVictim = bool.Parse(parts[7]);

            yield return new ResultPayload()
            {
                Id = id,
                SqlStartTime = sqlStartTime,
                DurationMs = sqlDuration,
                Prefix = prefix,
                Suffix = suffix,
                Spid = spid,
                IsDeadlockVictim = isDeadlockVictim
            };

            
        }
    }

}