package ru.spbstu;

import ru.spbstu.service.CompressedStorageService;
import ru.spbstu.service.SegmentMetadataService;
import ru.spbstu.service.SegmentStorageService;
import ru.spbstu.service.StorageService;
import ru.spbstu.storage.common.DataSource;
import ru.spbstu.storage.common.DataSourceFactory;
import ru.spbstu.storage.compressed.CompressedFilesDiskStorage;
import ru.spbstu.storage.metadata.SegmentMetadataDAO;
import ru.spbstu.storage.segments.SegmentsDiskStorage;
import ru.spbstu.util.PropsReader;

import java.sql.Connection;
import java.sql.SQLException;

public class DeduplicationApplication {

    private static final String POSTGRESQL_PROPS = "postgresql.properties";
    public static void main(String[] args) throws Exception {
        DataSource dataSource = DataSourceFactory.create(PropsReader.read(POSTGRESQL_PROPS));
        try (Connection connection = dataSource.createConnection()) {
            StorageService storageService = createStorageService(connection);
//             storageService.save(); // TODO: save multiple files at one time
            // calc stat of save
            // load();
            // calc stat of load
            printTestResults();
            // deleteAllData();
        }
    }

    private static StorageService createStorageService(Connection connection) throws SQLException {
        // segment metadata service
        final SegmentMetadataDAO segmentMetadataDAO = new SegmentMetadataDAO(connection);
        final SegmentMetadataService segmentMetadataService = new SegmentMetadataService(segmentMetadataDAO);

        // segment service
        final SegmentsDiskStorage segmentsDiskStorage = new SegmentsDiskStorage();
        final SegmentStorageService segmentStorageService = new SegmentStorageService(segmentsDiskStorage);

        // compressed service
        final CompressedFilesDiskStorage compressedFilesDiskStorage = new CompressedFilesDiskStorage();
        final CompressedStorageService compressedStorageService = new CompressedStorageService(compressedFilesDiskStorage);

        return new StorageService(segmentMetadataService, segmentStorageService, compressedStorageService);
    }

    private static void printTestResults() {

    }

}
