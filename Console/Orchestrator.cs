using Console;
using Microsoft.Win32.SafeHandles;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text;

namespace My1Brc;

static class Orchestrator
{
    public static string RunSequential(FileSegment[] segments, SafeFileHandle handle)
    {
        var aggregatedStations = new ConcurrentDictionary<string, AggregatedStationData>();

        foreach (var segment in segments)
        {
            var parser = new Parser();
            var parsedData = parser.Parse(segment, handle);

            Aggregate(parsedData, aggregatedStations);
        }

        return ToString(aggregatedStations);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static double Round(double number) => Math.Round(number, MidpointRounding.AwayFromZero);

    public static async Task<string> RunWithTasks(FileSegment[] segments, SafeFileHandle handle)
    {
        var aggregatedStations = new ConcurrentDictionary<string, AggregatedStationData>();
        var tasks = new Task[segments.Length];

        for (var i = 0; i < segments.Length; i++)
        {
            var segment = segments[i];
            tasks[i] = Task.Run(() =>
            {
                var parser = new Parser();
                var parsedData = parser.Parse(segment, handle);

                Aggregate(parsedData, aggregatedStations);
            });
        }

        await Task.WhenAll(tasks);

        return ToString(aggregatedStations);
    }

    public static async Task<string> RunWithTasksNoConcurency(FileSegment[] segments, SafeFileHandle handle)
    {
        var tasks = new Task<Dictionary<string, AggregatedStationData>>[segments.Length];

        for (var i = 0; i < segments.Length; i++)
        {
            var segment = segments[i];
            tasks[i] = Task.Run(() =>
            {
                var parser = new Parser();
                return parser.Parse(segment, handle);
            });
        }

        await Task.WhenAll(tasks);

        for (int i=1; i<tasks.Length; i++)
        {
            var task = tasks[i];
            Aggregate(task.Result, tasks[0].Result);
        }

        return ToString(tasks[0].Result);
    }

    public static async Task<string> RunWithTasksAndSharedDictionnary(FileSegment[] segments, SafeFileHandle handle)
    {
        var concurentDataAggregator = new ConcurentDataAggregator();
        var tasks = new Task[segments.Length];

        for (var i = 0; i < segments.Length; i++)
        {
            var segment = segments[i];
            tasks[i] = Task.Run(() =>
            {
                var parser = new Parser(concurentDataAggregator);
                _ = parser.Parse(segment, handle);
            });
        }

        await Task.WhenAll(tasks);

        return ToString(concurentDataAggregator.InternalConcurentDictionay.ToDictionary());
    }

    public static string RunWithThread(FileSegment[] segments, SafeFileHandle handle)
    {
        var aggregatedStations = new ConcurrentDictionary<string, AggregatedStationData>();
        var threads = new Thread[segments.Length].AsSpan();

        for(var i = 0; i < segments.Length; i++)
        {
            var segment = segments[i];
            var thread = new Thread(() =>
            {
                var parser = new Parser();
                var parsedData = parser.Parse(segment, handle);

                Aggregate(parsedData, aggregatedStations);
            })
            {
                Priority = ThreadPriority.Highest
            };

            threads[i] = thread;
            thread.Start();
        }

        foreach (var thread in threads)
        {
            thread.Join();
        }

        return ToString(aggregatedStations);
    }

    private static string ToString(IEnumerable<KeyValuePair<string, AggregatedStationData>> aggregatedStationDatas)
    {
        var resultStringBuilder = new StringBuilder();

        foreach (var station in aggregatedStationDatas.OrderBy(s => s.Key))
        {
            resultStringBuilder
                .Append($"{station.Key};{(double)station.Value.Min / 10};{Round(station.Value.Sum / station.Value.Count) / 10};{(double)station.Value.Max / 10}\n");
        }

        return resultStringBuilder.ToString();
    }

    private static void Aggregate(Dictionary<string, AggregatedStationData> segmentStations, ConcurrentDictionary<string, AggregatedStationData> globalStations)
    {
        foreach (var station in segmentStations)
        {
            globalStations.AddOrUpdate(station.Key, station.Value, (key, oldValue) =>
            {
                return new AggregatedStationData
                {
                    Count = oldValue.Count + station.Value.Count,
                    Max = Math.Max(oldValue.Max, station.Value.Max),
                    Min = Math.Min(oldValue.Min, station.Value.Min),
                    Sum = oldValue.Sum + station.Value.Sum
                };
            });
        }
    }

    private static void Aggregate(Dictionary<string, AggregatedStationData> segmentStations, Dictionary<string, AggregatedStationData> globalStations)
    {
        foreach (var station in segmentStations)
        {
            if(globalStations.TryGetValue(station.Key, out var aggregatedStationData))
            {
                aggregatedStationData.Count += station.Value.Count;
                aggregatedStationData.Max = Math.Max(aggregatedStationData.Max, station.Value.Max);
                aggregatedStationData.Min = Math.Min(aggregatedStationData.Min, station.Value.Min);
                aggregatedStationData.Sum += station.Value.Sum;
            }
            else
            {
                globalStations.Add(station.Key, station.Value);
            }
        }
    }
}