package ru.spbstu.hash;

import org.jetbrains.annotations.NotNull;

import java.lang.foreign.MemorySegment;
import java.util.Objects;

public class DiskSegment {

    private final String hash;
    private final MemorySegment memorySegment;

    public DiskSegment(@NotNull String hash,
                       @NotNull MemorySegment memorySegment) {
        this.hash = Objects.requireNonNull(hash);
        this.memorySegment = Objects.requireNonNull(memorySegment);
    }

    public String getHash() {
        return hash;
    }

    public MemorySegment getMemorySegment() {
        return memorySegment;
    }

}
