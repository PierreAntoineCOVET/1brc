using Console;
using Microsoft.Win32.SafeHandles;
using System.Collections.Concurrent;
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

        var resultStringBuilder = new StringBuilder();
        
        foreach (var station in aggregatedStations.Values.OrderBy(s => s.Name))
        {
            resultStringBuilder
                .Append($"{station.Name};{station.Min};{Math.Round((double)((station.Sum / 10) / station.Count), 1, MidpointRounding.AwayFromZero)};{station.Max}\n");
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
                    Name = key,
                    Sum = oldValue.Sum + station.Value.Sum
                };
            });
        }
    }
}