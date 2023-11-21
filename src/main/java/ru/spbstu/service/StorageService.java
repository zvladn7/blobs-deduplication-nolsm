package ru.spbstu.service;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.spbstu.exception.StorageException;
import ru.spbstu.hash.HashType;
import ru.spbstu.hash.SegmentUtil;
import ru.spbstu.hash.SegmentHashUtil;
import ru.spbstu.hash.MemorySegmentWithHash;
import ru.spbstu.model.SegmentMetadata;
import ru.spbstu.model.SegmentsMetadataToStore;
import ru.spbstu.storage.compressed.CompressedFileInfo;
import ru.spbstu.storage.util.DiskStorageUtil;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class StorageService {

    private static final Logger LOGGER = LoggerFactory.getLogger(StorageService.class);

    private final SegmentMetadataService segmentMetadataService;
    private final SegmentStorageService segmentStorageService;
    private final CompressedStorageService compressedStorageService;

    public StorageService(@NotNull SegmentMetadataService segmentMetadataService,
                          @NotNull SegmentStorageService segmentStorageService,
                          @NotNull CompressedStorageService compressedStorageService) {
        this.segmentMetadataService = Objects.requireNonNull(segmentMetadataService);
        this.segmentStorageService = Objects.requireNonNull(segmentStorageService);
        this.compressedStorageService = Objects.requireNonNull(compressedStorageService);
    }

    public void store(@NotNull Path path,
                      @NotNull HashType hashType) {
        // Разбили файл на сегменты и посчитали хеш для каждого сегмента
        final List<MemorySegmentWithHash> memorySegmentWithHashes = getHashToBytesSegmentMap(path, hashType);

        // Получили сегменты, которые уже есть на диске + которых еще не было на диске (то есть новые, уникальные сегменты)
        SegmentsMetadataToStore segmentsMetadataToStore = segmentMetadataService.getSegmentsMetadataToStore(memorySegmentWithHashes);
        logSegmentsToStore(segmentsMetadataToStore);

        // Записали новые сегменты на диск
        Map<String, SegmentMetadata> updatedNewSegmentsMetadataMap
                = segmentStorageService.saveNewSegmentsOnDisk(segmentsMetadataToStore, memorySegmentWithHashes);
        segmentsMetadataToStore = segmentsMetadataToStore.updateNewSegmentsMap(updatedNewSegmentsMetadataMap);

        // Записали мета-данные о новых сегментах на диск
        segmentMetadataService.create(new ArrayList<>(segmentsMetadataToStore.getNewSegmentsMap().values()));
        // Обновили мета-данные по уже существующим
        segmentMetadataService.updateReferenceCount(segmentsMetadataToStore.getAlreadyExistedSegmentsMap().values());

//        List<String> newSegmentHashes = segmentsMetadataToStore.getNewSegmentsMap().values().stream()
//                .map(SegmentMetadata::getHash)
//                .toList();
//
//        // У новых сегментов нет id в SegmentMetadata, поэтому запрашиваем сегменты.
//        updatedNewSegmentsMetadataMap = segmentMetadataService.findByHashes(newSegmentHashes);
//        segmentsMetadataToStore = segmentsMetadataToStore.updateNewSegmentsMap(updatedNewSegmentsMetadataMap);

        // Записали сжатый файл на диск.
        compressedStorageService.store(path, memorySegmentWithHashes, segmentsMetadataToStore.getAllHashToMetadataMap());
    }

    private List<MemorySegment> getFileSegments(@NotNull Path path) {
        try {
            return SegmentUtil.getSegmentsOfBytes(path);
        } catch (IOException e) {
            throw new StorageException(String.format("Failed to get file segments, file: %s", path.getFileName()));
        }
    }

    private List<MemorySegmentWithHash> getHashToBytesSegmentMap(@NotNull Path path,
                                                                 @NotNull HashType hashType) {
        List<MemorySegment> segmentsOfBytes = getFileSegments(path);
        try {
            return SegmentHashUtil.calculateHashes(segmentsOfBytes, hashType);
        } catch (NoSuchAlgorithmException e) {
            throw new StorageException("Failed to get hash of segments");
        }
    }

    private static void logSegmentsToStore(@NotNull SegmentsMetadataToStore segmentsMetadataToStore) {
        Objects.requireNonNull(segmentsMetadataToStore);
        System.out.println("duplicate segments: " + segmentsMetadataToStore.getDuplicateSegments());
        System.out.println("reused from db segments: " + segmentsMetadataToStore.getReusedFromDBSegments());
    }

    public void restore(@NotNull String fileName) {
        Objects.requireNonNull(fileName);
        CompressedFileInfo compressedFileInfo = compressedStorageService.readCompressedFileInfo(fileName);
        Map<Integer, SegmentMetadata> idToMetadataMap = segmentMetadataService.findByIds(compressedFileInfo.metadataIds());
        segmentStorageService.restore(compressedFileInfo, idToMetadataMap);
    }
}
