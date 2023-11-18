package ru.spbstu.service;

import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;
import ru.spbstu.exception.StorageException;
import ru.spbstu.hash.FileSegmentSlicer;
import ru.spbstu.hash.SegmentHasher;
import ru.spbstu.hash.SegmentHasherResult;
import ru.spbstu.model.SegmentMetadata;
import ru.spbstu.repository.SegmentRepository;
import ru.spbstu.storage.SegmentsDiskStorage;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class SegmentStorageService {

    private final SegmentRepository segmentRepository;
    private final SegmentsDiskStorage segmentsDiskStorage;

    public SegmentStorageService(SegmentRepository segmentRepository,
                                 SegmentsDiskStorage segmentsDiskStorage) {
        this.segmentRepository = segmentRepository;
        this.segmentsDiskStorage = segmentsDiskStorage;
    }

    public void save(@NotNull MultipartFile file) {
        final List<MemorySegment> fileSegments = getFileSegments(file);
        final List<SegmentHasherResult> segmentHasherResults = getHashToBytesSegmentMap(fileSegments, file.getName());
        final Map<String, MemorySegment> hashToBytesSegmentMap
                = segmentHasherResults.stream()
                .collect(Collectors.toMap(
                        SegmentHasherResult::getHash,
                        SegmentHasherResult::getMemorySegment,
                        (m1, m2) -> m1,
                        () -> new HashMap<>(segmentHasherResults.size() * 2)
                ));
        final Map<String, SegmentMetadata> segmentsFromDBMap = getAlreadyExistSegmentsFromDB(hashToBytesSegmentMap);

        Map<String, SegmentMetadata> segmentsToUpdateInDBMap
                = segmentsDiskStorage.calculateSegmentsToUpdateInDB(hashToBytesSegmentMap.keySet(), segmentsFromDBMap);

        segmentsToUpdateInDBMap = saveSegmentsOnDisk(segmentsToUpdateInDBMap, hashToBytesSegmentMap);

        segmentRepository.saveAllAndFlush(segmentsToUpdateInDBMap.values());

//        segmentsDiskStorage.saveCompressedDataFile(file.getName(), segmentHasherResults, segmentsToUpdateInDBMap);
    }

    private List<MemorySegment> getFileSegments(@NotNull MultipartFile file) {
        try {
            return FileSegmentSlicer.getSegmentsOfBytes(file);
        } catch (IOException e) {
            throw new StorageException(String.format("Failed to get file segments, file: %s", file.getName()));
        }
    }

    private List<SegmentHasherResult> getHashToBytesSegmentMap(@NotNull List<MemorySegment> segmentsOfBytes,
                                                               @NotNull String fileName) {
        try {
            return SegmentHasher.calculateHashes(segmentsOfBytes);
        } catch (NoSuchAlgorithmException e) {
            throw new StorageException(String.format("Failed to get hash of segments, file: %s", fileName));
        }
    }

    private Map<String, SegmentMetadata> getAlreadyExistSegmentsFromDB(@NotNull Map<String, MemorySegment> hashToBytesSegmentMap) {
        return segmentRepository.findAllById(hashToBytesSegmentMap.keySet()).stream()
                .collect(Collectors.toMap(
                        SegmentMetadata::getHash,
                        Function.identity(),
                        (m1, m2) -> m1,
                        HashMap::new
                ));
    }

    private Map<String, SegmentMetadata> saveSegmentsOnDisk(@NotNull Map<String, SegmentMetadata> segmentsToUpdateInDBMap,
                                                            @NotNull Map<String, MemorySegment> hashToBytesSegmentMap) {
        List<SegmentMetadata> segmentsToWriteOnDisk = segmentsToUpdateInDBMap.values().stream()
                .filter(SegmentMetadata::isUnknown)
                .toList();

        Map<String, SegmentMetadata> hashToSegmentStoredOnDisk;
        try {
            hashToSegmentStoredOnDisk = segmentsDiskStorage.saveSegmentsOnDisk(segmentsToWriteOnDisk, hashToBytesSegmentMap);
        } catch (IOException e) {
            throw new StorageException("Fail to store segments on disk");
        }

        return segmentsToUpdateInDBMap.entrySet().stream()
                .map(entry -> {
                    String hash = entry.getKey();
                    SegmentMetadata metadata = entry.getValue();
                    if (!metadata.isUnknown()) {
                        return entry;
                    }
                    SegmentMetadata newMetadata = hashToSegmentStoredOnDisk.get(hash);
                    if (newMetadata == null) {
                        throw new StorageException("");
                    }
                    entry.setValue(newMetadata);
                    return entry;
                })
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                ));
    }

}
