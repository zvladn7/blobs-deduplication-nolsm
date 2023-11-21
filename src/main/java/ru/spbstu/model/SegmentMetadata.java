package ru.spbstu.model;

import org.jetbrains.annotations.NotNull;

public class SegmentMetadata {

    private static final String FILE_UNKNOWN_NAME = "UNKNOWN";
    private static final long FILE_UNKNOWN_OFFSET = 0;
    private static final int DEFAULT_REFERENCE_COUNT = 1;

    private int id;

    @NotNull
    private String hash;

    @NotNull
    private String fileName;

    private long fileOffset;

    private int references;

    public SegmentMetadata() {}

    public SegmentMetadata(int id,
                           @NotNull String hash,
                           @NotNull String fileName,
                           long offset,
                           int references) {
        this.id = id;
        this.hash = hash;
        this.fileName = fileName;
        this.fileOffset = offset;
        this.references = references;
    }

    public SegmentMetadata(@NotNull String hash,
                           @NotNull String fileName,
                           long offset,
                           int references) {
        this.hash = hash;
        this.fileName = fileName;
        this.fileOffset = offset;
        this.references = references;
    }

    public SegmentMetadata(@NotNull String hash) {
        this.hash = hash;
        this.fileName = FILE_UNKNOWN_NAME;
        this.fileOffset = FILE_UNKNOWN_OFFSET;
        this.references = DEFAULT_REFERENCE_COUNT;
    }

    public boolean isUnknown() {
        return FILE_UNKNOWN_NAME.equals(fileName) && fileOffset == FILE_UNKNOWN_OFFSET;
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

    public long getFileOffset() {
        return fileOffset;
    }

    public void setFileOffset(long fileOffset) {
        this.fileOffset = fileOffset;
    }

    public int getReferences() {
        return references;
    }

    public void setReferences(int references) {
        this.references = references;
    }

    @Override
    public String toString() {
        return "SegmentMetadata{" +
                "id=" + id +
                ", hash='" + hash + '\'' +
                ", fileName='" + fileName + '\'' +
                ", offset=" + fileOffset +
                ", referenceCount=" + references +
                '}';
    }
}
