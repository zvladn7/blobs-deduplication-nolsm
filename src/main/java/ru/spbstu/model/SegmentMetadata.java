package ru.spbstu.model;

import org.jetbrains.annotations.NotNull;
import org.thymeleaf.util.StringUtils;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

@Entity
@Table(
        name = "SegmentsMetadata",
        indexes = {
                @Index(name = "segment_metadata_hash_idx", columnList = "hash", unique = true)
        }
)
public class SegmentMetadata {

    private static final String FILE_UNKNOWN_NAME = "UNKNOWN";
    private static final long FILE_UNKNOWN_OFFSET = 0;
    private static final int DEFAULT_REFERENCE_COUNT = 1;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private int id;

    @NotNull
    @Column(name = "hash", unique = true, nullable = false, updatable = false)
    private String hash;

    @NotNull
    @Column(name = "fileName", nullable = false, updatable = false)
    private String fileName;

    @Column(name = "offset", nullable = false, updatable = false)
    private long offset;

    @Column(name = "referenceCount", nullable = false, updatable = true)
    private int referenceCount;

    public SegmentMetadata() {}

    public SegmentMetadata(int id,
                           @NotNull String hash,
                           @NotNull String fileName,
                           long offset,
                           int referenceCount) {
        this.id = id;
        this.hash = hash;
        this.fileName = fileName;
        this.offset = offset;
        this.referenceCount = referenceCount;
    }

    public SegmentMetadata(@NotNull String hash,
                           @NotNull String fileName,
                           long offset,
                           int referenceCount) {
        this.hash = hash;
        this.fileName = fileName;
        this.offset = offset;
        this.referenceCount = referenceCount;
    }

    public SegmentMetadata(@NotNull String hash) {
        this.hash = hash;
        this.fileName = FILE_UNKNOWN_NAME;
        this.offset = FILE_UNKNOWN_OFFSET;
        this.referenceCount = DEFAULT_REFERENCE_COUNT;
    }

    public boolean isUnknown() {
        return StringUtils.equals(fileName, FILE_UNKNOWN_NAME) && offset == FILE_UNKNOWN_OFFSET;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @NotNull
    public String getHash() {
        return hash;
    }

    public void setHash(@NotNull String hash) {
        this.hash = hash;
    }

    @NotNull
    public String getFileName() {
        return fileName;
    }

    public void setFileName(@NotNull String fileName) {
        this.fileName = fileName;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public int getReferenceCount() {
        return referenceCount;
    }

    public void setReferenceCount(int referenceCount) {
        this.referenceCount = referenceCount;
    }

    @Override
    public String toString() {
        return "SegmentMetadata{" +
                "id=" + id +
                ", hash='" + hash + '\'' +
                ", fileName='" + fileName + '\'' +
                ", offset=" + offset +
                ", referenceCount=" + referenceCount +
                '}';
    }
}
