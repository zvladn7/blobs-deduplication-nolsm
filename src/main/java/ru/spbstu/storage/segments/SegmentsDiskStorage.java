package ru.spbstu.storage.segments;

import org.jetbrains.annotations.NotNull;
import ru.spbstu.exception.StorageException;
import ru.spbstu.model.SegmentMetadata;
import ru.spbstu.storage.compressed.CompressedFileInfo;
import ru.spbstu.storage.util.DiskStorageUtil;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SegmentsDiskStorage {

    private static final Path INDEX = DiskStorageUtil.ofSegment("index.idx");
    private static final Path INDEX_TMP = DiskStorageUtil.ofSegment("index.tmp");

    private final Arena arena = Arena.ofConfined();
    private volatile Map<String, MemorySegment> segmentsMap;

    public SegmentsDiskStorage() throws IOException {
        this.segmentsMap = load(arena);
    }

    public void close() {
        if (!arena.scope().isAlive()) {
            return;
        }
        arena.close();
    }

    public static Map<String, MemorySegment> load(@NotNull Arena arena) throws IOException {
        if (!Files.exists(INDEX)) {
            if (Files.exists(INDEX_TMP)) {
                Files.move(INDEX_TMP, INDEX, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } else {
                Files.createFile(INDEX);
            }
        }

        List<String> existedFiles = Files.readAllLines(INDEX, StandardCharsets.UTF_8);
        Map<String, MemorySegment> results = HashMap.newHashMap(existedFiles.size());
        for (String fileName : existedFiles) {
            Path segmentsListPath = DiskStorageUtil.ofSegment(fileName);
            try (FileChannel fileChannel = FileChannel.open(segmentsListPath, StandardOpenOption.READ)) {
                results.put(fileName, fileChannel.map(
                        FileChannel.MapMode.READ_ONLY,
                        0,
                        Files.size(segmentsListPath),
                        arena
                ));
            }
        }
        return Collections.unmodifiableMap(results);
    }

    public Map<String, SegmentMetadata> saveNewSegmentsOnDisk(@NotNull Collection<SegmentMetadata> segmentMetadatas,
                                                              @NotNull Map<String, MemorySegment> hashToBytesSegmentMap) throws IOException {
        try {
            Files.createFile(INDEX);
        } catch (FileAlreadyExistsException ignored) {
            // it's ok actually it's normal state
        }

        List<String> existedFiles = Files.readAllLines(INDEX, StandardCharsets.UTF_8);
        String newFileName = String.valueOf(existedFiles.size());

        long dataSize = 0;
        for (SegmentMetadata segmentMetadata : segmentMetadatas) {
            String hash = segmentMetadata.getHash();
            MemorySegment memorySegment = hashToBytesSegmentMap.get(hash);
            if (memorySegment == null) {
                throw new StorageException(String.format("No bytes for segment with hash: %s", hash));
            }
            dataSize += memorySegment.byteSize();
        }

        Map<String, SegmentMetadata> result = new HashMap<>();

        try (FileChannel fileChannel = FileChannel.open(
                DiskStorageUtil.ofSegment(newFileName),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);
             Arena writeArena = Arena.ofConfined()
        ) {
            MemorySegment fileSegment = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    dataSize,
                    writeArena
            );

            long dataOffset = 0;
            for (SegmentMetadata segmentMetadata : segmentMetadatas) {
                String hash = segmentMetadata.getHash();
                MemorySegment nextSegment = hashToBytesSegmentMap.get(hash);
                MemorySegment.copy(nextSegment, 0, fileSegment, dataOffset, nextSegment.byteSize());

                segmentMetadata.setFileName(newFileName);
                segmentMetadata.setFileOffset(dataOffset);
                result.put(hash, segmentMetadata);

                dataOffset += nextSegment.byteSize();
            }
        }

        List<String> list = new ArrayList<>(existedFiles.size() + 1);
        list.addAll(existedFiles);
        list.add(newFileName);
        Files.write(
                INDEX_TMP,
                list,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        Files.deleteIfExists(INDEX);

        Files.move(INDEX_TMP, INDEX, StandardCopyOption.ATOMIC_MOVE);

        this.segmentsMap = load(arena);

        return result;
    }

    public void decompressFile(@NotNull MemorySegment decompressedFileMemorySegment,
                               @NotNull CompressedFileInfo compressedFileInfo,
                               @NotNull Map<Integer, SegmentMetadata> idToMetadataMap) {
        List<Integer> metadataIds = compressedFileInfo.metadataIds();
        long segmentSizeInBytes = compressedFileInfo.segmentSizeInBytes();
        long fileSizeInBytes = compressedFileInfo.fileSizeInBytes();
        long decompressedFileOffset = 0;
        int count = 0;
        for (Integer metadataId : metadataIds) {
            try {
                SegmentMetadata segmentMetadata = idToMetadataMap.get(metadataId);
                String srcSegmentFileName = segmentMetadata.getFileName();
                long srcSegmentOffset = segmentMetadata.getFileOffset();
                MemorySegment srcSegment = segmentsMap.get(srcSegmentFileName);
                long currentSegmentSizeInBytes;
                if (decompressedFileOffset + segmentSizeInBytes > fileSizeInBytes) {
                    currentSegmentSizeInBytes = fileSizeInBytes - decompressedFileOffset;
                    System.out.println(currentSegmentSizeInBytes);
                } else {
                    currentSegmentSizeInBytes = segmentSizeInBytes;
                }
                MemorySegment.copy(
                        srcSegment,
                        srcSegmentOffset,
                        decompressedFileMemorySegment,
                        decompressedFileOffset,
                        currentSegmentSizeInBytes
                );
                decompressedFileOffset += currentSegmentSizeInBytes;
                count++;
            } catch (RuntimeException e) {
                System.out.println("metadataId: " + metadataId + ", metadataIds: " + metadataIds.size() + ", count: " + count);
                throw new StorageException("", e);
            }
        }
        System.out.println(decompressedFileOffset);
    }

}
