using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static System.Collections.Specialized.BitVector32;

namespace Console;

internal class ConcurentDataAggregator : IDataAggregator
{
    public ConcurrentDictionary<string, AggregatedStationData> InternalConcurentDictionay { get; } = new ConcurrentDictionary<string, AggregatedStationData>();
    public Dictionary<string, AggregatedStationData> InternalDictionay => null;

    public void Aggregate(StationData stationData)
    {
        InternalConcurentDictionay.AddOrUpdate(stationData.Name,
            new AggregatedStationData
            {
                Count = 1,
                Max = stationData.Temp,
                Min = stationData.Temp,
                Sum = stationData.Temp,
            },
            (key, oldValue) =>
            {
                return new AggregatedStationData
                {
                    Count = oldValue.Count + 1,
                    Max = Math.Max(oldValue.Max, stationData.Temp),
                    Min = Math.Min(oldValue.Min, stationData.Temp),
                    Sum = oldValue.Sum + stationData.Temp
                };
            });
    }
}
