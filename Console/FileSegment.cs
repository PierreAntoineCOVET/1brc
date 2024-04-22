namespace My1Brc;

record struct FileSegment(long Offset, long Length)
{
    public long End => Offset + Length;
}