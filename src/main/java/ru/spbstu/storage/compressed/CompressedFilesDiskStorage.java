package ru.spbstu.storage.compressed;

import org.jetbrains.annotations.NotNull;
import ru.spbstu.exception.StorageException;
import ru.spbstu.hash.MemorySegmentWithHash;
import ru.spbstu.model.SegmentMetadata;
import ru.spbstu.storage.util.DiskStorageUtil;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CompressedFilesDiskStorage {

    /**
     * compressed file format:
     * |segment size in bytes|segments count|segment0|segment1|segment2|...
     */
    public void saveCompressedDataFile(@NotNull String fileName,
                                       @NotNull List<MemorySegmentWithHash> memorySegmentWithHashes,
                                       @NotNull Map<String, SegmentMetadata> segmentsToUpdateInDBMap) throws IOException {
        int compressedDataSize = memorySegmentWithHashes.size() * Integer.BYTES;

        try (FileChannel fileChannel = FileChannel.open(
                DiskStorageUtil.ofCompressed(fileName),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);
             Arena writeArena = Arena.ofConfined();
        ) {
            MemorySegment fileSegment = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    compressedDataSize,
                    writeArena
            );

            int dataOffset = 0;
            long segmentSizeInBytes = memorySegmentWithHashes.get(0).getMemorySegment().byteSize();
            fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset, segmentSizeInBytes);
            dataOffset += Long.BYTES;

            int segmentsCount = memorySegmentWithHashes.size();
            fileSegment.set(ValueLayout.JAVA_INT_UNALIGNED, dataOffset, segmentsCount);
            dataOffset += Integer.BYTES;

            long fileSizeInBytes = memorySegmentWithHashes.stream()
                    .map(MemorySegmentWithHash::getMemorySegment)
                    .map(MemorySegment::byteSize)
                    .mapToLong(Long::longValue)
                    .sum();
            fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset, fileSizeInBytes);
            dataOffset += Long.BYTES;

            for (MemorySegmentWithHash hasherResult : memorySegmentWithHashes) {
                String hash = hasherResult.getHash();
                SegmentMetadata metadata = segmentsToUpdateInDBMap.get(hash);
                int segmentId = metadata.getId();
                fileSegment.set(ValueLayout.JAVA_INT_UNALIGNED, dataOffset, segmentId);
                dataOffset += Integer.BYTES;
            }
        }
    }

    public CompressedFileInfo readCompressedFileInfo(@NotNull String fileName) throws IOException {
        Path compressedFilePath = DiskStorageUtil.ofCompressed(fileName);
        try (FileChannel fileChannel = FileChannel.open(compressedFilePath, StandardOpenOption.READ);
             Arena arena = Arena.ofConfined();
        ) {
            MemorySegment memorySegment
                    = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(compressedFilePath), arena);

            long segmentSizeInBytes = memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
            int segmentsCount = memorySegment.get(ValueLayout.JAVA_INT_UNALIGNED, Long.BYTES);
            long fileSizeInBytes = memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES + Integer.BYTES);
            List<Integer> metadataIds = new ArrayList<>(segmentsCount);
            int baseOffset = 2 * Long.BYTES + Integer.BYTES;
            for (int offsetIdx = 0; offsetIdx < segmentsCount; ++offsetIdx) {
                int dataOffset = baseOffset + offsetIdx * Integer.BYTES;
                int metadataId = memorySegment.get(ValueLayout.JAVA_INT_UNALIGNED, dataOffset);
                metadataIds.add(metadataId);
            }
            if (segmentsCount != metadataIds.size()) {
                throw new StorageException(
                        String.format("Segments count and metadata size isn't equal on data read, file: %s", fileChannel));
            }
            return new CompressedFileInfo(compressedFilePath.getFileName().toString(), segmentSizeInBytes, segmentsCount, fileSizeInBytes, metadataIds);
        }
    }

    public void decompressStoredFile(@NotNull CompressedFileInfo compressedFileInfo,
                                     @NotNull Map<Integer, SegmentMetadata> idToMetadataMap) {
        String s = compressedFileInfo.compressedFileName();
        List<Integer> metadataIds = compressedFileInfo.metadataIds();
        long segmentSizeInBytes = compressedFileInfo.segmentSizeInBytes();
        long fileSizeInBytes = compressedFileInfo.fileSizeInBytes();

//        try (FileChannel fileChannel = FileChannel.open(
//
//        )) {
//
//        }
    }

}
