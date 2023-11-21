package ru.spbstu.util;

import org.jetbrains.annotations.NotNull;
import ru.spbstu.exception.StorageException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.util.Properties;

public class PropsReader {

    private static final Path RESOURCE_FOLDER = Path.of(PropsReader.class.getClassLoader().getResource("").getFile())
            .resolve("./../../../resources/main");

    private PropsReader() {}

    public static Properties read(@NotNull String propsFileName) throws IOException {
        Properties props = new Properties();
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        try (InputStream resourceStream = loader.getResourceAsStream(propsFileName)) {
            props.load(resourceStream);
        }
//        try (InputStream inputStream = new FileInputStream(RESOURCE_FOLDER.resolve(propsFileName).toFile())) {
//            props.load(inputStream);
//        }
        return props;
    }

}
