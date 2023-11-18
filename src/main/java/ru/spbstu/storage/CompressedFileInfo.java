package ru.spbstu.storage;

import org.jetbrains.annotations.NotNull;

import java.util.List;

public record CompressedFileInfo(@NotNull String compressedFileName,
                                 long segmentSizeInBytes,
                                 int segmentsCount,
                                 long fileSizeInBytes,
                                 @NotNull List<Integer> metadataIds) {
}
