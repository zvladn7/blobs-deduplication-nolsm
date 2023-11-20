package ru.spbstu.service;

import org.jetbrains.annotations.NotNull;
import ru.spbstu.exception.StorageException;
import ru.spbstu.hash.MemorySegmentWithHash;
import ru.spbstu.model.SegmentMetadata;
import ru.spbstu.model.SegmentsMetadataToStore;
import ru.spbstu.storage.segments.SegmentsDiskStorage;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SegmentStorageService {

    private final SegmentsDiskStorage diskStorage;

    public SegmentStorageService(SegmentsDiskStorage diskStorage) {
        this.diskStorage = diskStorage;
    }

    public Map<String, SegmentMetadata> saveNewSegmentsOnDisk(@NotNull SegmentsMetadataToStore segmentsMetadataToStore,
                                                              @NotNull List<MemorySegmentWithHash> memorySegmentWithHashes) {
        Collection<SegmentMetadata> segmentsToWriteOnDiskMetadataList = segmentsMetadataToStore.getNewSegmentsMap().values();
        Map<String, MemorySegment> hashToMemorySegmentMap = memorySegmentWithHashes.stream()
                .collect(Collectors.toMap(
                        MemorySegmentWithHash::getHash,
                        MemorySegmentWithHash::getMemorySegment,
                        (m1, m2) -> m1,
                        HashMap::new
                ));

        Map<String, SegmentMetadata> hashToSegmentStoredOnDisk;
        try {
            hashToSegmentStoredOnDisk = diskStorage.saveNewSegmentsOnDisk(segmentsToWriteOnDiskMetadataList, hashToMemorySegmentMap);
        } catch (IOException e) {
            throw new StorageException("Fail to store segments on disk");
        }
        return segmentsMetadataToStore.getNewSegmentsMap().entrySet().stream()
                .map(entry -> {
                    String hash = entry.getKey();
                    SegmentMetadata metadata = entry.getValue();
                    if (!metadata.isUnknown()) {
                        return entry;
                    }
                    SegmentMetadata newMetadata = hashToSegmentStoredOnDisk.get(hash);
                    if (newMetadata == null) {
                        throw new StorageException(String.format("No metadata found for hash %s", hash));
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
