package ru.spbstu.util;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropsReader {

    private PropsReader() {}

    public static Properties read(@NotNull String propsFileName) throws IOException {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Properties props = new Properties();
        try (InputStream resourceStream = loader.getResourceAsStream(propsFileName)) {
            props.load(resourceStream);
        }
        return props;
    }

}
