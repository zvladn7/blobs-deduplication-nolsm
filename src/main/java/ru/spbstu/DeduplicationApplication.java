package ru.spbstu;

import org.jetbrains.annotations.NotNull;
import ru.spbstu.exception.StorageException;
import ru.spbstu.hash.HashType;
import ru.spbstu.service.CompressedStorageService;
import ru.spbstu.service.SegmentMetadataService;
import ru.spbstu.service.SegmentStorageService;
import ru.spbstu.service.StorageService;
import ru.spbstu.storage.common.DataSource;
import ru.spbstu.storage.common.DataSourceFactory;
import ru.spbstu.storage.compressed.CompressedFilesDiskStorage;
import ru.spbstu.storage.metadata.SegmentMetadataDAO;
import ru.spbstu.storage.segments.SegmentsDiskStorage;
import ru.spbstu.storage.util.DiskStorageUtil;
import ru.spbstu.util.PropsReader;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.stream.Stream;

public class DeduplicationApplication {

    private static final String POSTGRESQL_PROPS = "postgres.properties";
    private static final String DATASET_INPUT = "data/input";

    public static void main(String[] args) throws Exception {
        DataSource dataSource = DataSourceFactory.create(PropsReader.read(POSTGRESQL_PROPS));
        try (Connection connection = dataSource.createConnection()) {
            StorageService storageService = createStorageService(connection);
            storeDataSet(storageService);
            decompressData(storageService);
            compareResults();

            // calc stat of save
            // load();
            // calc stat of load
            // deleteAllData();
        }
    }

    private static void decompressData(@NotNull StorageService storageService) throws URISyntaxException, IOException {
        Objects.requireNonNull(storageService);
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        URL resource = loader.getResource(DATASET_INPUT);
        if (resource == null) {
            throw new StorageException("Fail to get dataset resource");
        }
        try (Stream<Path> files = Files.list(Path.of(resource.toURI()))) {
            files.forEach(nextDataSetFile -> storageService.restore(nextDataSetFile.getFileName().toString()));
        }
    }

    private static void storeDataSet(@NotNull StorageService storageService) throws URISyntaxException, IOException {
        Objects.requireNonNull(storageService);
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        URL resource = loader.getResource(DATASET_INPUT);
        if (resource == null) {
            throw new StorageException("Fail to get dataset resource");
        }
        try (Stream<Path> files = Files.list(Path.of(resource.toURI()))) {
            files.forEach(nextDataSetFile -> storageService.store(nextDataSetFile, HashType.SHA256));
        }
    }

    private static void compareResults() throws IOException {
        try (Stream<Path> files = Files.list(DiskStorageUtil.input())) {
            files.forEach(nextInputFile -> {
                String fileName = nextInputFile.getFileName().toString();
                try {
                    if (Files.mismatch(DiskStorageUtil.ofInput(fileName), DiskStorageUtil.ofDecompressed(fileName)) != -1) {
                        throw new StorageException(String.format("Files not equals, name=%s", fileName));
                    }
                } catch (IOException e) {
                    throw new StorageException("", e);
                }
            });
        }
    }

    private static StorageService createStorageService(Connection connection) throws IOException {
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
