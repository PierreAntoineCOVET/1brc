﻿using System.Diagnostics;
using System.Text;
using My1Brc;

// Inspired from https://github.com/nietras/1brc.cs

var sw = new Stopwatch();
sw.Start();

var fileName = "measurements_1M.txt";
var filePath = Path.Combine("C:\\Users\\pierr\\source\\repos\\1brc\\Data", fileName);
var fileHandle = File.OpenHandle(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);

var fileSegments = FileSegmenter.GetSegments(fileHandle, Environment.ProcessorCount);

Orchestrator.RunSequential(fileSegments, fileHandle);

sw.Stop();
Console.WriteLine($"In Main {sw.ElapsedMilliseconds,6} ms");

Span<byte> buffer = stackalloc byte[128];

foreach (var segment in fileSegments)
{
    Console.WriteLine(segment);

    RandomAccess.Read(fileHandle, buffer, segment.Offset);
    var startOfSegment = Encoding.UTF8.GetString(buffer);

    RandomAccess.Read(fileHandle, buffer, segment.End - 128);
    var endOfSegment = Encoding.UTF8.GetString(buffer);

    Console.WriteLine(startOfSegment);
    Console.WriteLine(endOfSegment);
    Console.WriteLine();
}

Console.WriteLine($"File length {RandomAccess.GetLength(fileHandle)} ms");