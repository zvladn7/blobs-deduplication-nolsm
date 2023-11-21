package ru.spbstu.storage.util;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.nio.file.Path;
import java.util.Objects;

public class DiskStorageUtil {

    private static final String BASE_DATA_PATH = "./src/main/resources/data";
    private static final Path INPUT_DATA_PATH = Path.of(BASE_DATA_PATH + File.separator + "input");
    private static final Path SEGMENTS_DATA_PATH = Path.of(BASE_DATA_PATH + File.separator + "segments");
    private static final Path COMPRESSED_DATA_PATH = Path.of(BASE_DATA_PATH + File.separator + "compressed");
    private static final Path DECOMPRESSED_DATA_PATH = Path.of(BASE_DATA_PATH + File.separator + "decompressed");
    private static final String COMPRESSED_FILE_POSTFIX = ".bin";


    public static Path input() {
        return INPUT_DATA_PATH;
    }

    public static Path ofInput(@NotNull String fileName) {
        return INPUT_DATA_PATH.resolve(Objects.requireNonNull(fileName));
    }

    public static Path ofSegment(@NotNull String fileName) {
        return SEGMENTS_DATA_PATH.resolve(Objects.requireNonNull(fileName));
    }

    public static Path ofCompressed(@NotNull String fileName) {
        return COMPRESSED_DATA_PATH.resolve(Objects.requireNonNull(fileName) + COMPRESSED_FILE_POSTFIX);
    }

    public static String getFileNameFromCompressed(@NotNull String fileName) {
        return fileName.substring(0, fileName.indexOf(COMPRESSED_FILE_POSTFIX));
    }

    public static Path ofDecompressed(@NotNull String fileName) {
        return DECOMPRESSED_DATA_PATH.resolve(Objects.requireNonNull(fileName));
    }

}
