using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Console;

internal class DataAggregator : IDataAggregator
{
    public Dictionary<string, AggregatedStationData> InternalDictionay { get; } = new Dictionary<string, AggregatedStationData>();

    public void Aggregate(StationData stationData)
    {
        if (InternalDictionay.TryGetValue(stationData.Name, out var existingLine))
        {
            existingLine.Count++;
            existingLine.Max = Math.Max(existingLine.Max, stationData.Temp);
            existingLine.Min = Math.Min(existingLine.Min, stationData.Temp);
            existingLine.Sum += stationData.Temp;
        }
        else
        {
            InternalDictionay.Add(stationData.Name, new AggregatedStationData
            {
                Count = 1,
                Max = stationData.Temp,
                Min = stationData.Temp,
                Sum = stationData.Temp
            });
        }
    }
}
