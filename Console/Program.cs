﻿using System.Diagnostics;
using System.Text;
using My1Brc;
using static System.Collections.Specialized.BitVector32;

// Inspired from https://github.com/nietras/1brc.cs

var sw = new Stopwatch();
sw.Start();

Process.GetCurrentProcess().PriorityClass = ProcessPriorityClass.RealTime;
System.Console.OutputEncoding = Encoding.UTF8;

string filePath;

if(args.Length > 0)
{
    filePath = args[0];
}
else
{
    var fileName = "measurements_10K.txt";
    //var fileName = "measurements_1M.txt";
    //var fileName = "measurements_1B.txt";
    filePath = Path.Combine("C:\\Users\\pacovet\\1brcData", fileName);
}

var fileHandle = File.OpenHandle(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);

var fileSegments = FileSegmenter.GetSegments(fileHandle, Environment.ProcessorCount);

var aggregatedData = args.Length > 1
    ? args[1] switch
    {
        "seq" => Orchestrator.RunSequential(fileSegments, fileHandle),
        "task" => await Orchestrator.RunWithTasks(fileSegments, fileHandle),
        "thread" => Orchestrator.RunWithThread(fileSegments, fileHandle),
        "naive" => await Naive(filePath),
        _ => throw new Exception($"Execution mode {args[1]} not supported. Use 'seq', 'task', 'thread', 'naive'.")
    }
    : Orchestrator.RunSequential(fileSegments, fileHandle);

System.Console.Write(aggregatedData);

sw.Stop();
System.Console.WriteLine($"In Main {sw.ElapsedMilliseconds,6} ms");

async Task<string> Naive(string filePath)
{
    var naiveLines = new Dictionary<string, NaiveLine>();

    await foreach (string line in File.ReadLinesAsync(filePath))
    {
        int separatorIndex = line.IndexOf(";");

        string name = line.Substring(0, separatorIndex);
        double temp = double.Parse(line.Substring(separatorIndex + 1));

        if(naiveLines.TryGetValue(name, out NaiveLine naiveLine))
        {
            naiveLine.Min = Math.Min(temp, naiveLine.Min);
            naiveLine.Max = Math.Max(temp, naiveLine.Max);
            naiveLine.Count++;
            naiveLine.Sum += temp;
        }
        else
        {
            naiveLines.Add(
                name,
                new NaiveLine
                {
                    Min = temp,
                    Max = temp,
                    Count = 1,
                    Sum = temp
                });
        }
    }

    var stringBuilder = new StringBuilder();

    foreach (var naiveLine in naiveLines.OrderBy(nl => nl.Key))
    {
        stringBuilder
                .Append($"{naiveLine.Key};{naiveLine.Value.Min};{Math.Round(naiveLine.Value.Sum / naiveLine.Value.Count, MidpointRounding.AwayFromZero)};{naiveLine.Value.Max}\n");
    }

    return stringBuilder.ToString();
}

class NaiveLine
{
    public double Min { get; set; }
    public double Max { get; set; }
    public double Sum { get; set; }
    public int Count { get; set; }
}