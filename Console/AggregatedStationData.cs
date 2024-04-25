using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Console
{
    public record struct AggregatedStationData
    {
        public string Name;
        public short Min;
        public short Max;
        public long Sum;
        public int Count;
    }
}
