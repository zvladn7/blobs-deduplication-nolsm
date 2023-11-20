package ru.spbstu.storage.metadata;

import org.jetbrains.annotations.NotNull;
import ru.spbstu.model.SegmentMetadata;
import ru.spbstu.storage.executor.RowReader;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static ru.spbstu.storage.metadata.SegmentMetadataTable.C_FILE_NAME;
import static ru.spbstu.storage.metadata.SegmentMetadataTable.C_FILE_OFFSET;
import static ru.spbstu.storage.metadata.SegmentMetadataTable.C_HASH;
import static ru.spbstu.storage.metadata.SegmentMetadataTable.C_ID;
import static ru.spbstu.storage.metadata.SegmentMetadataTable.C_REFERENCE;

public class SegmentMetadataRowReader implements RowReader<List<SegmentMetadata>> {

    public static final SegmentMetadataRowReader INSTANCE = new SegmentMetadataRowReader();

    private SegmentMetadataRowReader() {}

    @Override
    public List<SegmentMetadata> handle(@NotNull ResultSet rs) throws SQLException {
        Objects.requireNonNull(rs);
        List<SegmentMetadata> metadataList = new ArrayList<>();
        while (rs.next()) {
            int id = rs.getInt(C_ID);
            String hash = rs.getString(C_HASH);
            String fileName = rs.getString(C_FILE_NAME);
            long fileOffset = rs.getLong(C_FILE_OFFSET);
            int references = rs.getInt(C_REFERENCE);
            metadataList.add(new SegmentMetadata(
                    id,
                    hash,
                    fileName,
                    fileOffset,
                    references
            ));
        }
        return metadataList;
    }
}
