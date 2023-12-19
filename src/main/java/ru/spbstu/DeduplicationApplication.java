package ru.spbstu;

import org.jetbrains.annotations.NotNull;
import ru.spbstu.exception.StorageException;
import ru.spbstu.hash.HashType;
import ru.spbstu.model.SegmentsMetadataToStore;
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
import ru.spbstu.util.Context;
import ru.spbstu.util.PropsReader;
import ru.spbstu.util.StatInfo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.Objects;
import java.util.stream.Stream;

public class DeduplicationApplication {

    private static final HashType HASH_TYPE = HashType.SHA256;
    private static final String POSTGRESQL_PROPS = "postgres.properties";
    private static final String DATASET_INPUT = "data/input";

    public static void main(String[] args) throws Exception {
        DataSource dataSource = DataSourceFactory.create(PropsReader.read(POSTGRESQL_PROPS));
        try (Connection connection = dataSource.createConnection()) {
            Context context = new Context(HashType.SHA256, 32);
            StatInfo statInfo = new StatInfo();
            StorageService storageService = createStorageService(connection);
            storeDataSet(storageService, context, statInfo);
            decompressData(storageService, statInfo);
            compareResults();
            writeStatInfo(context, statInfo);
        }
    }

    private static void decompressData(@NotNull StorageService storageService,
                                       @NotNull StatInfo statInfo) throws URISyntaxException, IOException {
        Objects.requireNonNull(storageService);
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        URL resource = loader.getResource(DATASET_INPUT);
        if (resource == null) {
            throw new StorageException("Fail to get dataset resource");
        }
        long startInMillis = System.currentTimeMillis();
        try (Stream<Path> files = Files.list(Path.of(resource.toURI()))) {
            files.forEach(nextDataSetFile -> {
                long startLocalInMillis = System.currentTimeMillis();
                storageService.restore(nextDataSetFile.getFileName().toString());
                long timeLocalInMillis = System.currentTimeMillis() - startLocalInMillis;
                if (statInfo.maxBlockReadTimeInMillis < timeLocalInMillis) {
                    statInfo.maxBlockReadTimeInMillis = timeLocalInMillis;
                }
                if (statInfo.minBlockReadTimeInMillis > timeLocalInMillis) {
                    statInfo.minBlockReadTimeInMillis = timeLocalInMillis;
                }
            });
        }
        statInfo.fullBlockReadTimeInMillis = System.currentTimeMillis() - startInMillis;
    }

    private static void storeDataSet(@NotNull StorageService storageService,
                                     @NotNull Context context,
                                     @NotNull StatInfo statInfo) throws URISyntaxException, IOException {
        Objects.requireNonNull(storageService);
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        URL resource = loader.getResource(DATASET_INPUT);
        if (resource == null) {
            throw new StorageException("Fail to get dataset resource");
        }
        long startInMillis = System.currentTimeMillis();
        try (Stream<Path> files = Files.list(Path.of(resource.toURI()))) {
            files.forEach(nextDataSetFile -> {
                long startLocalInMillis = System.currentTimeMillis();
                SegmentsMetadataToStore store = storageService.store(nextDataSetFile, context);
                statInfo.duplicates += store.getDuplicateSegments();
                statInfo.unique += store.getNewSegmentsMap().size();
                long timeLocalInMillis = System.currentTimeMillis() - startLocalInMillis;
                if (statInfo.maxBlockWriteTimeInMillis < timeLocalInMillis) {
                    statInfo.maxBlockWriteTimeInMillis = timeLocalInMillis;
                }
                if (statInfo.minBlockWriteTimeInMillis > timeLocalInMillis) {
                    statInfo.minBlockWriteTimeInMillis = timeLocalInMillis;
                }
            });
        }
        statInfo.fullBlockWriteTimeInMillis = System.currentTimeMillis() - startInMillis;
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

    private static void writeStatInfo(@NotNull Context context,
                                      @NotNull StatInfo statInfo) {
        String base = "src/main/resources/data/stat";
        String fileName = base + "/" + context.hashType().name() + "_" + context.segmentSizeInBytes();
        System.out.println(Path.of(fileName).toAbsolutePath().toString());
        try (Writer writer = new BufferedWriter(new FileWriter(fileName))) {
            writer.append("WRITE STAT INFO:").append("\n");
            writer.append("minBlockWriteTimeInMillis: ")
                    .append(String.valueOf(statInfo.minBlockWriteTimeInMillis))
                    .append("\n");
            writer.append("maxBlockWriteTimeInMillis: ")
                    .append(String.valueOf(statInfo.maxBlockWriteTimeInMillis))
                    .append("\n");
            writer.append("fullBlockWriteTimeInMillis: ")
                    .append(String.valueOf(statInfo.fullBlockWriteTimeInMillis))
                    .append("\n");
            writer.append("duplicates: ")
                    .append(String.valueOf(statInfo.duplicates))
                    .append("\n");
            writer.append("unique: ")
                    .append(String.valueOf(statInfo.unique))
                    .append("\n");

            writer.append("READ STAT INFO:").append("\n");
            writer.append("minBlockReadTimeInMillis: ")
                    .append(String.valueOf(statInfo.minBlockReadTimeInMillis))
                    .append("\n");
            writer.append("maxBlockReadTimeInMillis: ")
                    .append(String.valueOf(statInfo.maxBlockReadTimeInMillis))
                    .append("\n");
            writer.append("fullBlockReadTimeInMillis: ")
                    .append(String.valueOf(statInfo.fullBlockReadTimeInMillis))
                    .append("\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
