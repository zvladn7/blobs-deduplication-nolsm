package ru.spbstu.model;

import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SegmentsMetadataToStore {

    private final Map<String, SegmentMetadata> newSegmentsMap;
    private final Map<String, SegmentMetadata> alreadyExistedSegmentsMap;
    private final int reusedFromDBSegments;
    private final int duplicateSegments;

    public SegmentsMetadataToStore(@NotNull Map<String, SegmentMetadata> newSegmentsMap,
                                   @NotNull Map<String, SegmentMetadata> alreadyExistedSegmentsMap,
                                   int duplicateSegments,
                                   int reusedFromDBSegments) {
        this.newSegmentsMap = Objects.requireNonNull(newSegmentsMap);
        this.alreadyExistedSegmentsMap = Objects.requireNonNull(alreadyExistedSegmentsMap);
        this.reusedFromDBSegments = reusedFromDBSegments;
        this.duplicateSegments = duplicateSegments;
    }

    @NotNull
    public Map<String, SegmentMetadata> getNewSegmentsMap() {
        return newSegmentsMap;
    }

    @NotNull
    public Map<String, SegmentMetadata> getAlreadyExistedSegmentsMap() {
        return alreadyExistedSegmentsMap;
    }

    public int getReusedFromDBSegments() {
        return reusedFromDBSegments;
    }

    public int getDuplicateSegments() {
        return duplicateSegments;
    }

    public SegmentsMetadataToStore updateNewSegmentsMap(@NotNull Map<String, SegmentMetadata> updatedNewSegmentsMap) {
        return new SegmentsMetadataToStore(
                updatedNewSegmentsMap,
                alreadyExistedSegmentsMap,
                reusedFromDBSegments,
                duplicateSegments
        );
    }

    public Map<String, SegmentMetadata> getAllHashToMetadataMap() {
        Map<String, SegmentMetadata> map = HashMap.newHashMap(newSegmentsMap.size() + alreadyExistedSegmentsMap.size());
        map.putAll(newSegmentsMap);
        map.putAll(alreadyExistedSegmentsMap);
        return map;
    }

}
