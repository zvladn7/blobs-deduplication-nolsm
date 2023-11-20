package ru.spbstu.storage.metadata;

import org.jetbrains.annotations.NotNull;
import org.thymeleaf.util.Validate;
import ru.spbstu.model.SegmentMetadata;
import ru.spbstu.storage.executor.DataBaseRequestExecutor;
import ru.spbstu.storage.executor.DefaultPreparedStatementUpdater;

import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static ru.spbstu.storage.metadata.SegmentMetadataTable.C_FILE_NAME;
import static ru.spbstu.storage.metadata.SegmentMetadataTable.C_FILE_OFFSET;
import static ru.spbstu.storage.metadata.SegmentMetadataTable.C_HASH;
import static ru.spbstu.storage.metadata.SegmentMetadataTable.C_ID;
import static ru.spbstu.storage.metadata.SegmentMetadataTable.C_REFERENCE;
import static ru.spbstu.storage.metadata.SegmentMetadataTable.TABLE_NAME;

public class SegmentMetadataDAO {

    private static final String INSERT_STMT = "INSERT INTO " + TABLE_NAME + "(" +
            C_HASH + ", " +
            C_FILE_NAME + ", " +
            C_FILE_OFFSET + ", " +
            C_REFERENCE + ")" +
            " VALUES (?, ?, ?, ?)";

    private static final String UPDATE_REFERENCE_COUNT_STMT = "UPDATE "  + TABLE_NAME + " SET " +
            C_REFERENCE + " = ? " +
            "WHERE " + C_ID + " = ?";


    private static final String QUERY_METADATA = "SELECT * FROM " + TABLE_NAME;
    private static final String QUERY_METADATA_BY_IDS = QUERY_METADATA + "WHERE id in (?)";
    private static final String QUERY_METADATA_BY_HASHES = QUERY_METADATA + "WHERE hash in (?)";

    public final DataBaseRequestExecutor dbRequestExecutor;

    public SegmentMetadataDAO(Connection connection) {
        this.dbRequestExecutor = new DataBaseRequestExecutor(connection);
    }

    public void create(@NotNull SegmentMetadata metadata) {
        Objects.requireNonNull(metadata);
        int updated = dbRequestExecutor.executeUpdate(INSERT_STMT, ps -> {
            ps.setString(1, metadata.getHash());
            ps.setString(2, metadata.getFileName());
            ps.setLong(3, metadata.getFileOffset());
            ps.setInt(4, metadata.getReferences());
        });
        Validate.isTrue(updated == 1, String.format("Metadata [%s} wasn't inserted", metadata));
    }

    public void updateReferenceCount(int id,
                                     int newReferenceCount) {
        int updated = dbRequestExecutor.executeUpdate(UPDATE_REFERENCE_COUNT_STMT, ps -> {
            ps.setInt(1, newReferenceCount);
            ps.setInt(2, id);
        });
        Validate.isTrue(updated == 1,
                String.format("Metadata %d wasn't updated, references=%d", id, newReferenceCount));
    }

    @NotNull
    public List<SegmentMetadata> findAll() {
        return dbRequestExecutor.executeQuery(
                QUERY_METADATA,
                DefaultPreparedStatementUpdater.INSTANCE,
                SegmentMetadataRowReader.INSTANCE
        );
    }

    @NotNull
    public List<SegmentMetadata> findAllByIds(@NotNull Collection<Integer> metadataIds) {
        String metadataIdsString = Objects.requireNonNull(metadataIds).stream()
                .map(String::valueOf)
                .collect(Collectors.joining(","));
        return dbRequestExecutor.executeQuery(
                QUERY_METADATA_BY_IDS,
                ps -> ps.setString(1, metadataIdsString),
                SegmentMetadataRowReader.INSTANCE
        );
    }

    @NotNull
    public List<SegmentMetadata> findAllByHashes(@NotNull Collection<String> metadataHashes) {
        String metadataHashesString = String.join(",", metadataHashes);
        return dbRequestExecutor.executeQuery(
                QUERY_METADATA_BY_HASHES,
                ps -> ps.setString(1, metadataHashesString),
                SegmentMetadataRowReader.INSTANCE
        );
    }

}
