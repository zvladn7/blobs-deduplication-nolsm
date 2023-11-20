package ru.spbstu.storage.metadata;

public interface SegmentMetadataTable {

    String TABLE_NAME = "segments_metadata";

    String C_ID = "id";
    String C_HASH = "hash";
    String C_FILE_NAME = "file_name";
    String C_FILE_OFFSET = "file_offset";
    String C_REFERENCE = "reference";

}
