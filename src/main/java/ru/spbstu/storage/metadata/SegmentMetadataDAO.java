package ru.spbstu.storage.metadata;

import com.google.common.base.Joiner;
import org.jetbrains.annotations.NotNull;
import ru.spbstu.exception.StorageException;
import ru.spbstu.model.SegmentMetadata;
import ru.spbstu.storage.executor.DataBaseRequestExecutor;
import ru.spbstu.storage.executor.DefaultPreparedStatementUpdater;

import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static ru.spbstu.storage.metadata.SegmentMetadataTable.C_FILE_NAME;
import static ru.spbstu.storage.metadata.SegmentMetadataTable.C_FILE_OFFSET;
import static ru.spbstu.storage.metadata.SegmentMetadataTable.C_HASH;
import static ru.spbstu.storage.metadata.SegmentMetadataTable.C_ID;
import static ru.spbstu.storage.metadata.SegmentMetadataTable.C_REFERENCE;
import static ru.spbstu.storage.metadata.SegmentMetadataTable.TABLE_NAME;

public class SegmentMetadataDAO {

    private static final String INSERT_STMT = "INSERT INTO " + TABLE_NAME + " (" +
            C_HASH + ", " +
            C_FILE_NAME + ", " +
            C_FILE_OFFSET + ", " +
            C_REFERENCE + ")" +
            " VALUES (?, ?, ?, ?)";

    private static final String UPDATE_REFERENCE_COUNT_STMT = "UPDATE "  + TABLE_NAME + " SET " +
            C_REFERENCE + " = ? " +
            "WHERE " + C_ID + " = ?";


    private static final String QUERY_METADATA = "SELECT * FROM " + TABLE_NAME;
    private static final String QUERY_METADATA_BY_IDS = QUERY_METADATA + " WHERE id = ANY (?)";
    private static final String QUERY_METADATA_BY_HASHES = QUERY_METADATA + " WHERE hash = ANY (?)";

    public final DataBaseRequestExecutor dbRequestExecutor;

    public SegmentMetadataDAO(Connection connection) {
        this.dbRequestExecutor = new DataBaseRequestExecutor(connection);
    }

    public void createBatch(@NotNull List<SegmentMetadata> metadataList) {
        Objects.requireNonNull(metadataList);
        List<Integer> generatedIds = dbRequestExecutor.executeCreate(INSERT_STMT, ps -> {
            for (SegmentMetadata metadata : metadataList) {
                ps.setString(1, metadata.getHash());
                ps.setString(2, metadata.getFileName());
                ps.setLong(3, metadata.getFileOffset());
                ps.setInt(4, metadata.getReferences());
                ps.addBatch();
            }
        });
        if (generatedIds.size() != metadataList.size()) {
            throw new StorageException("Generated ids size isn't equals to metadata list size");
        }
        for (int idx = 0; idx < generatedIds.size(); ++idx) {
            Integer metadataId = generatedIds.get(idx);
            metadataList.get(idx).setId(metadataId);
        }
    }
    public void updateBatchReferenceCount(@NotNull Collection<SegmentMetadata> metadataList) {
        Objects.requireNonNull(metadataList);
        dbRequestExecutor.executeUpdate(UPDATE_REFERENCE_COUNT_STMT, ps -> {
            for (SegmentMetadata metadata : metadataList) {
                ps.setInt(1, metadata.getReferences());
                ps.setInt(2, metadata.getId());
                ps.addBatch();
            }
        });
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
        return dbRequestExecutor.executeQuery(
                QUERY_METADATA_BY_IDS,
                ps -> ps.setArray(
                        1,
                        dbRequestExecutor.createArray("INTEGER", metadataIds.toArray())
                ),
                SegmentMetadataRowReader.INSTANCE
        );
    }

    @NotNull
    public List<SegmentMetadata> findAllByHashes(@NotNull Collection<String> metadataHashes) {

        return dbRequestExecutor.executeQuery(
                QUERY_METADATA_BY_HASHES,
                ps -> ps.setArray(
                        1,
                        dbRequestExecutor.createArray("VARCHAR", metadataHashes.toArray())
                ),
                SegmentMetadataRowReader.INSTANCE
        );
    }

}
