package ru.spbstu.storage.common;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.Properties;

public class DataSourceFactory {

    private static final String DB_HOST = "db.host";
    private static final String DB_PORT = "db.port";
    private static final String DB_NAME = "db.name";
    private static final String DB_USER = "db.user";
    private static final String DB_PASSWORD = "db.password";

    private DataSourceFactory() {}

    @NotNull
    public static DataSource create(@NotNull Properties properties) {
        String host = extractAndValidateStringProp(properties, DB_HOST);
        int port = extractAndValidateIntProp(properties, DB_PORT);
        String name = extractAndValidateStringProp(properties, DB_NAME);
        String user = extractAndValidateStringProp(properties, DB_USER);
        String password = extractAndValidateStringProp(properties, DB_PASSWORD);

        return new DataSource(
                host,
                port,
                name,
                user,
                password);
    }

    private static String extractAndValidateStringProp(@NotNull Properties properties,
                                                       @NotNull String propertyKey) {
        Objects.requireNonNull(properties);
        Objects.requireNonNull(propertyKey);
        String propertyValue = (String) properties.get(propertyKey);
        return Objects.requireNonNull(propertyValue);
    }

    private static int extractAndValidateIntProp(@NotNull Properties properties,
                                                       @NotNull String propertyKey) {
        Objects.requireNonNull(properties);
        Objects.requireNonNull(propertyKey);
        String propertyValue = (String) properties.get(propertyKey);
        return Integer.parseInt(propertyValue);
    }

}
