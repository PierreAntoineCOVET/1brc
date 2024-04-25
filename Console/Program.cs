﻿using System.Diagnostics;
using My1Brc;

// Inspired from https://github.com/nietras/1brc.cs

var sw = new Stopwatch();
sw.Start();

var fileName = "measurements_10K.txt";
//var fileName = "measurements_1M.txt";
//var fileName = "measurements_1B.txt";
var filePath = Path.Combine("C:\\Users\\pacovet\\1brcData", fileName);
var fileHandle = File.OpenHandle(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);

var fileSegments = FileSegmenter.GetSegments(fileHandle, Environment.ProcessorCount);

//var aggregatedData = Orchestrator.RunSequential(fileSegments, fileHandle);
var aggregatedData = await Orchestrator.RunWithTasks(fileSegments, fileHandle);

System.Console.Write(aggregatedData);

sw.Stop();
System.Console.WriteLine($"In Main {sw.ElapsedMilliseconds,6} ms");

//foreach (var segment in fileSegments)
//{
//    System.Console.WriteLine($"{segment.Offset} - {segment.Offset + segment.Length}");
//}
