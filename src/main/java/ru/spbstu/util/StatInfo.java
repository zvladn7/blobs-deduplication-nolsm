package ru.spbstu.util;

public class StatInfo {

    // write stat info
    public long maxBlockWriteTimeInMillis = Long.MIN_VALUE;
    public long minBlockWriteTimeInMillis = Long.MAX_VALUE;
    public long fullBlockWriteTimeInMillis;
    public long duplicates;
    public long unique;

    // read stat info
    public long maxBlockReadTimeInMillis = Long.MIN_VALUE;
    public long minBlockReadTimeInMillis = Long.MAX_VALUE;
    public long fullBlockReadTimeInMillis;


}
