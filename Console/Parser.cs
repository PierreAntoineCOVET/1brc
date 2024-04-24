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

    public void Parse(FileSegment fileSegment, SafeFileHandle fileHandle)
    {
        foreach (var byteLine in GetByteLines(fileSegment, fileHandle))
        {
            Aggregate(byteLine);
        }        
    }

    private void Aggregate(StationData line)
    {
        System.Console.WriteLine(Encoding.UTF8.GetString(line.Name));
        System.Console.WriteLine(Encoding.UTF8.GetString(line.Temp));
    }

    private static IEnumerable<StationData> GetByteLines(FileSegment fileSegment, SafeFileHandle fileHandle)
    {
        var results = new List<StationData>();

        long fileLength = RandomAccess.GetLength(fileHandle);

        var currentFilePosition = fileSegment.Offset;
        var endOfSegment = fileSegment.End;

        Span<byte> buffer = stackalloc byte[ReadBufferSize];

        while (currentFilePosition < endOfSegment)
        {
            RandomAccess.Read(fileHandle, buffer, currentFilePosition);

            var lastLineFeed = buffer.LastIndexOf(LineFeed) + LineFeed.Length;
            var lastFilePosition = 0;

            while (lastFilePosition < lastLineFeed)
            {
                var nextLineFeed = buffer.IndexOf(LineFeed) + LineFeed.Length;
                var currentLine = buffer.Slice(lastFilePosition, nextLineFeed);

                var separator = currentLine.IndexOf(Separator);
                var numberStartPosition = separator + Separator.Length;

                var plop = currentLine.Slice(0, separator).ToArray();
                var plops = Encoding.UTF8.GetString(plop);

                var plip = currentLine.Slice(numberStartPosition, currentLine.Length - numberStartPosition).ToArray();
                var plips = Encoding.UTF8.GetString(plip);

                //results.Add(new StationData
                //{
                //    Name = currentLine.Slice(0, separator).ToArray(),
                //    Temp = currentLine.Slice(numberStartPosition, currentLine.Length - numberStartPosition).ToArray();
                //    //TODO pase number as short rather than decimal, store the number of number after the dot
                //    // unicode number are always on 1 byte
                //    // Look to extract the sign also ?
                //});

                lastFilePosition += nextLineFeed;
            }
        }

        return results;
    }
}