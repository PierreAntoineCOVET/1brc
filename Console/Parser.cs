using Microsoft.Win32.SafeHandles;
using System;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Runtime.Intrinsics;
using System.Text;
using System.Threading.Tasks;

namespace My1Brc;

static class Parser
{
    public unsafe void Parse(FileSegment fileSegment, SafeFileHandle fileHandle)
    {
        long fileLength = RandomAccess.GetLength(fileHandle);

        using var memoryMappedFile = MemoryMappedFile.CreateFromFile(
            fileHandle, null, fileLength,
            MemoryMappedFileAccess.Read, HandleInheritability.None, true);

        using var viewAccessor = memoryMappedFile.CreateViewAccessor(fileSegment.Offset, fileSegment.Length, MemoryMappedFileAccess.Read);

        using var memoryMappedViewHandle = viewAccessor.SafeMemoryMappedViewHandle;

        byte* memoryMapPointer = null;
        memoryMappedViewHandle.AcquirePointer(ref memoryMapPointer);

        var currentPosition = fileSegment.Offset;
        var endOfSegment = fileSegment.End;

        while (currentPosition < endOfSegment)
        {
            var data = Vector256.Load()
        }
    }
}