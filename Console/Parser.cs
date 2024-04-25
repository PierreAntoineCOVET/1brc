using Console;
using Microsoft.Win32.SafeHandles;
using System;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Runtime.Intrinsics;
using System.Text;
using System.Threading.Tasks;

namespace My1Brc;

class Parser
{
    private const int ReadBufferSize = 512;
    private static readonly byte[] LineFeed = Encoding.UTF8.GetBytes("\n");
    private static readonly byte[] Separator = Encoding.UTF8.GetBytes(";");
    private static readonly byte[] DecimalPoint = Encoding.UTF8.GetBytes(".");

    public Dictionary<string, AggregatedStationData> Parse(FileSegment fileSegment, SafeFileHandle fileHandle)
    {
        var aggregatedLines = new Dictionary<string, AggregatedStationData>();

        foreach (var byteLine in GetStationDataFromByteLine(fileSegment, fileHandle))
        {
            Aggregate(byteLine, aggregatedLines);
        }

        return aggregatedLines;
    }

    private static void Aggregate(StationData line, Dictionary<string, AggregatedStationData> parsedLines)
    {
        if (parsedLines.TryGetValue(line.Name, out var existingLine))
        {
            existingLine.Count++;
            existingLine.Max = Math.Max(existingLine.Max, line.Temp);
            existingLine.Min = Math.Min(existingLine.Min, line.Temp);
            existingLine.Sum += line.Temp;

            parsedLines[line.Name] = existingLine;
        }
        else
        {
            parsedLines.Add(line.Name, new AggregatedStationData
            {
                Count = 1,
                Max = line.Temp,
                Min = line.Temp,
                Name = line.Name,
                Sum = line.Temp
            });
        }
    }

    /// <summary>
    /// Parse every line of the file segment into a <see cref="StationData"/>.
    /// </summary>
    /// <param name="fileSegment">File segment to parse.</param>
    /// <param name="fileHandle">Handle to the actual file.</param>
    /// <returns>Enumerable of parsed station data.</returns>
    private IEnumerable<StationData> GetStationDataFromByteLine(FileSegment fileSegment, SafeFileHandle fileHandle)
    {
        var stationDatas = new List<StationData>();
        long fileLength = RandomAccess.GetLength(fileHandle);

        var currentFilePosition = fileSegment.Offset;
        var endOfSegment = fileSegment.End;

        Span<byte> buffer = stackalloc byte[ReadBufferSize];

        while (currentFilePosition < endOfSegment)
        {
            var bufferSize = RandomAccess.Read(fileHandle, buffer, currentFilePosition);

            var lastLineFeed = buffer.LastIndexOf(LineFeed) + LineFeed.Length;
            var lastBufferPosition = 0;
            var lastReadablePosition = Math.Min(bufferSize, lastLineFeed);

            while (lastBufferPosition < lastReadablePosition)
            {
                var nextLineFeed = buffer.Slice(lastBufferPosition).IndexOf(LineFeed) + LineFeed.Length;
                var currentLine = buffer.Slice(lastBufferPosition, nextLineFeed);

                var separator = currentLine.IndexOf(Separator);

                var numberStartPosition = separator + Separator.Length;

                var station = new StationData
                {
                    Name = Encoding.UTF8.GetString(currentLine.Slice(0, separator).ToArray()),
                    Temp = ParseTemp(currentLine.Slice(numberStartPosition))
                };
                stationDatas.Add(station);

                lastBufferPosition += nextLineFeed;
            }

            currentFilePosition += lastLineFeed;
        }

        return stationDatas;
    }

    /// <summary>
    /// Parse the temp into a short. Spec define all number to be 1 number after decimal.
    /// </summary>
    /// <param name="byteTemp">Line temperature in byte.</param>
    /// <returns>The temp as short.</returns>
    private static short ParseTemp(Span<byte> byteTemp)
    {
        var newTemp = new List<byte>(byteTemp.Length - DecimalPoint.Length);

        var decimalPointIndex = byteTemp.IndexOf(DecimalPoint);

        var indexAfterDecimal = decimalPointIndex + DecimalPoint.Length;

        newTemp.AddRange(byteTemp.Slice(0, decimalPointIndex));
        newTemp.AddRange(byteTemp.Slice(indexAfterDecimal));

        return short.Parse(Encoding.UTF8.GetString(newTemp.ToArray()));
    }
}