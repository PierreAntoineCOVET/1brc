using Console;
using Microsoft.Win32.SafeHandles;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Runtime.CompilerServices;
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

    private readonly IDataAggregator DataAggregator;

    public Parser(IDataAggregator dataAggregator = null)
    {

        this.DataAggregator = dataAggregator != null
            ? dataAggregator
            : new DataAggregator();
    }

    public Dictionary<string, AggregatedStationData> Parse(FileSegment fileSegment, SafeFileHandle fileHandle)
    {
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

                var station = new StationData(
                    Encoding.UTF8.GetString(currentLine.Slice(0, separator)),
                    ParseTemp(currentLine.Slice(numberStartPosition))
                );
                DataAggregator.Aggregate(station);

                lastBufferPosition += nextLineFeed;
            }

            currentFilePosition += lastLineFeed;
        }

        return DataAggregator.InternalDictionay;
    }

    /// <summary>
    /// Parse the temp into a short. Spec define all number to be 1 number after decimal.
    /// </summary>
    /// <param name="byteTemp">Line temperature in byte.</param>
    /// <returns>The temp as short.</returns>
    private static short ParseTemp(Span<byte> byteTemp)
    {
        var decimalPointIndex = byteTemp.IndexOf(DecimalPoint);

        var indexAfterDecimal = decimalPointIndex + DecimalPoint.Length;

        var tempString = Encoding.UTF8.GetString(byteTemp.Slice(0, decimalPointIndex))
            + Encoding.UTF8.GetString(byteTemp.Slice(indexAfterDecimal, byteTemp.Length - indexAfterDecimal - 2));

        return short.Parse(tempString);
    }
}