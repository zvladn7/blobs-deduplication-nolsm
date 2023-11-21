package ru.spbstu.storage.compressed;

import java.util.List;

public record CompressedFileInfo(String compressedFileName,
                                 long segmentSizeInBytes,
                                 int segmentsCount,
                                 long fileSizeInBytes,
                                 List<Integer> metadataIds) {
}
