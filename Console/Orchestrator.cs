using Microsoft.Win32.SafeHandles;

namespace My1Brc;

static class Orchestrator
{
    public static void RunSequential(FileSegment[] segments, SafeFileHandle handle)
    {
        foreach (var segment in segments)
        {
            var parser = new Parser();
            parser.Parse(segment, handle);
        }
    }
}