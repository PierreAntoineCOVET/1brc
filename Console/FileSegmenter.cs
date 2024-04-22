﻿using System.Text;
using Microsoft.Win32.SafeHandles;

namespace My1Brc;

static class FileSegmenter
{
    private static readonly byte[] LineFeed = Encoding.UTF8.GetBytes("\n");
    private const int ReadBufferSize = 256;

    /// <summary>
    /// Divide the given file into segments, one for each thread, adjusted for end of line.
    /// </summary>
    /// <param name="fileHandle">Handle to the file to divide.</param>
    /// <param name="numberOfThread">Number of segment to create.</param>
    /// <returns>Array of segments.</returns>
    public static FileSegment[] GetSegments(SafeFileHandle fileHandle, int numberOfThread)
    {
        long fileLength = RandomAccess.GetLength(fileHandle);

        var fileSegment = GetFileSegment(fileHandle, numberOfThread, fileLength);

        fileSegment = AdjustForEndOfLine(fileHandle, fileSegment, fileLength);

        return fileSegment;
    }

    /// <summary>
    /// Adjust the length of each sugment but the last to ensure that the split will always
    /// be sone after end of line.
    /// </summary>
    /// <param name="fileHandle">Handle to the targeted file.</param>
    /// <param name="fileSegments">Unadjusted segments for the file.</param>
    /// <param name="fileLength">Length of the target file.</param>
    /// <returns>Array of adjusted segments.</returns>
    private static FileSegment[] AdjustForEndOfLine(SafeFileHandle fileHandle, FileSegment[] fileSegments, long fileLength)
    {
        var lineFeedSpan = new Span<byte>(LineFeed);
        Span<byte> buffer = stackalloc byte[ReadBufferSize];

        for (int i = 0; i < fileSegments.Length - 1; i++)
        {
            long fileOffset = fileSegments[i].End - ReadBufferSize;

            RandomAccess.Read(fileHandle, buffer, fileOffset);

            var endOfLineIndex = buffer.IndexOf(lineFeedSpan);

            fileSegments[i].Length = fileOffset - fileSegments[i].Offset + endOfLineIndex + lineFeedSpan.Length;
            fileSegments[i + 1].Offset = fileSegments[i].End;
        }

        fileSegments[^1].Length = fileLength - fileSegments[^1].Offset;

        return fileSegments;
    }

    /// <summary>
    /// Divide the file in equal segments based on the number of thread available for on the proc.
    /// Adjust the last segment to account for the division rounding errors.
    /// </summary>
    /// <param name="fileHandle">Handle to the targeted file.</param>
    /// <param name="numberOfThread">Number of threads.</param>
    /// <param name="fileLength">Length of the target file.</param>
    /// <returns>File segments not accounting for end of lines.</returns>
    private static FileSegment[] GetFileSegment(SafeFileHandle fileHandle, int numberOfThread, long fileLength)
    {
        long fileSegmentLenght = fileLength / numberOfThread;

        var segments = new FileSegment[numberOfThread];

        for (var i = 0; i < numberOfThread; i++)
        {
            segments[i] = new FileSegment(i * fileSegmentLenght, fileSegmentLenght);
        }

        segments[^1].Length = fileLength - segments[^1].Offset;

        return segments;
    }
}