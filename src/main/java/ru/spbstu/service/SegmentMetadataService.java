package ru.spbstu.service;

import org.jetbrains.annotations.NotNull;
import ru.spbstu.exception.StorageException;
import ru.spbstu.hash.MemorySegmentWithHash;
import ru.spbstu.model.SegmentMetadata;
import ru.spbstu.model.SegmentsMetadataToStore;
import ru.spbstu.storage.metadata.SegmentMetadataDAO;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SegmentMetadataService {

    private final SegmentMetadataDAO dao;

    public SegmentMetadataService(@NotNull SegmentMetadataDAO dao) {
        this.dao = Objects.requireNonNull(dao);
    }

    public void create(@NotNull Collection<SegmentMetadata> metadataList) {
        Objects.requireNonNull(metadataList);
        for (SegmentMetadata segmentMetadata : metadataList) {
            dao.create(segmentMetadata);
        }
    }

    public void updateReferenceCount(@NotNull Collection<SegmentMetadata> metadataList) {
        Objects.requireNonNull(metadataList);
        for (SegmentMetadata segmentMetadata : metadataList) {
            if (segmentMetadata.isUnknown()) {
                throw new StorageException(
                        String.format("Metadata cannot be unknown on data update, id=%d", segmentMetadata.getId()));
            }
            int id = segmentMetadata.getId();
            int newReferenceCount = segmentMetadata.getReferences();
            dao.updateReferenceCount(id, newReferenceCount);
        }
    }

    @NotNull
    public List<SegmentMetadata> finaAll() {
        return dao.findAll();
    }

    @NotNull
    public Map<String, SegmentMetadata> findByHashes(@NotNull Collection<String> metadataHashes) {
        Objects.requireNonNull(metadataHashes);
        return dao.findAllByHashes(metadataHashes)
                .stream()
                .collect(Collectors.toMap(SegmentMetadata::getHash, Function.identity()));
    }

    @NotNull
    public Map<Integer, SegmentMetadata> findByIds(@NotNull Collection<Integer> metadataIds) {
        Objects.requireNonNull(metadataIds);
        return dao.findAllByIds(metadataIds)
                .stream()
                .collect(Collectors.toMap(SegmentMetadata::getId, Function.identity()));
    }

    @NotNull
    public SegmentsMetadataToStore getSegmentsMetadataToStore(@NotNull List<MemorySegmentWithHash> memorySegmentWithHashes) {
        List<String> segmentHashList = distinctHashList(memorySegmentWithHashes);

        Map<String, SegmentMetadata> segmentsFromDBMap = findByHashes(segmentHashList);
        Map<String, SegmentMetadata> newSegmentsMap = HashMap.newHashMap(segmentHashList.size());
        Map<String, SegmentMetadata> alreadyExistedSegmentsMap = HashMap.newHashMap(segmentHashList.size());

        int reusedFromDBSegments = 0;
        int duplicateSegments = 0;
        for (String segmentHash : segmentHashList) {
            SegmentMetadata segmentFromDB = segmentsFromDBMap.get(segmentHash);
            // Сегмент уже есть на диске
            if (segmentFromDB != null) {
                alreadyExistedSegmentsMap.put(
                        segmentHash,
                        new SegmentMetadata(segmentFromDB.getId(), segmentHash, segmentFromDB.getFileName(),
                                segmentFromDB.getFileOffset(), segmentFromDB.getReferences() + 1));
                reusedFromDBSegments++;
                duplicateSegments++;
                continue;
            }

            SegmentMetadata segmentFromCurrentFile = newSegmentsMap.get(segmentHash);
            // Сегмент уже был в этом файле
            if (segmentFromCurrentFile != null) {
                newSegmentsMap.put(
                        segmentHash,
                        new SegmentMetadata(segmentHash, segmentFromCurrentFile.getFileName(),
                                segmentFromCurrentFile.getFileOffset(), segmentFromCurrentFile.getReferences() + 1));
                duplicateSegments++;
                continue;
            }

            newSegmentsMap.put(segmentHash, new SegmentMetadata(segmentHash));
        }
        return new SegmentsMetadataToStore(
                newSegmentsMap,
                alreadyExistedSegmentsMap,
                duplicateSegments,
                reusedFromDBSegments
        );
    }

    private static List<String> distinctHashList(@NotNull List<MemorySegmentWithHash> memorySegmentWithHashes) {
        return memorySegmentWithHashes.stream().map(MemorySegmentWithHash::getHash).distinct().toList();
    }

}
