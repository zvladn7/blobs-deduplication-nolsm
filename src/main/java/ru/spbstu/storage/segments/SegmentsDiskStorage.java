package ru.spbstu.storage.segments;

import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;
import ru.spbstu.exception.StorageException;
import ru.spbstu.model.SegmentMetadata;
import ru.spbstu.storage.util.DiskStorageUtil;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
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

@Component
public class SegmentsDiskStorage {

    private static final Path INDEX = DiskStorageUtil.ofSegment("index.idx");
    private static final Path INDEX_TMP = DiskStorageUtil.ofSegment("index.tmp");

    private final Arena arena = Arena.ofConfined();
    private volatile Map<String, MemorySegment> segmentsMap;

    @PostConstruct
    public void init() throws IOException {
        this.segmentsMap = load(arena);
    }

    @PreDestroy
    public void destroy() {
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

    public Map<String, SegmentMetadata> saveSegmentsOnDisk(@NotNull Collection<SegmentMetadata> segmentMetadatas,
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
                MemorySegment.copy(nextSegment, 0, fileSegment, 0, nextSegment.byteSize());

                segmentMetadata.setFileName(newFileName);
                segmentMetadata.setOffset(dataOffset);
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


    public Map<String, SegmentMetadata> calculateSegmentsToUpdateInDB(@NotNull Collection<String> fileHashes,
                                                                      @NotNull Map<String, SegmentMetadata> segmentsFromDBMap) {
        Map<String, SegmentMetadata> segmentsFromCurrentFileMap = HashMap.newHashMap(fileHashes.size());
        Map<String, SegmentMetadata> segmentsToSaveMap = HashMap.newHashMap(fileHashes.size());

        int reusedFromDBSegments = 0;
        int duplicateSegments = 0;
        for (String segmentHash : fileHashes) {
            SegmentMetadata segmentFromDB = segmentsFromDBMap.get(segmentHash);
            // Сегмент уже сохранен на диске
            if (segmentFromDB != null) {
                segmentsToSaveMap.put(segmentHash, new SegmentMetadata(segmentFromDB.getId(), segmentHash, segmentFromDB.getFileName(),
                        segmentFromDB.getOffset(), segmentFromDB.getReferenceCount() + 1));
                reusedFromDBSegments++;
                duplicateSegments++;
                continue;
            }

            SegmentMetadata segmentFromCurrentFile = segmentsFromCurrentFileMap.get(segmentHash);
            if (segmentFromCurrentFile != null) {
                segmentFromCurrentFile = new SegmentMetadata(segmentHash, segmentFromCurrentFile.getFileName(),
                        segmentFromCurrentFile.getOffset(), segmentFromCurrentFile.getReferenceCount() + 1);
                segmentsToSaveMap.put(segmentHash, segmentFromCurrentFile);
                segmentsFromCurrentFileMap.put(segmentHash, segmentFromCurrentFile);
                duplicateSegments++;
                continue;
            }

            segmentsToSaveMap.put(segmentHash, new SegmentMetadata(segmentHash));
        }
        return segmentsToSaveMap;
    }

    public void readSegmentsFromDisk() {

    }

}
