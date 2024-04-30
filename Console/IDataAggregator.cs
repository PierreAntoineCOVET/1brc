using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Console
{
    internal interface IDataAggregator
    {
        public void Aggregate(StationData stationData);

        public Dictionary<string, AggregatedStationData> InternalDictionay { get; }
    }
}
