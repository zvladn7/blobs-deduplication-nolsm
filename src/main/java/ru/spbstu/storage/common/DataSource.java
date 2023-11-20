package ru.spbstu.storage.common;

import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;

public class DataSource {

    private static final String JDBC_URL_TEMPLATE = "jdbc:postgresql://%s:%d/%s";
    private static final String USER = "user";
    private static final String PASSWORD = "password";

    private final String host;
    private final int port;
    private final String name;
    private final String user;
    private final String password;

    public DataSource(@NotNull String host,
                      int port,
                      @NotNull String name,
                      @NotNull String user,
                      @NotNull String password) {
        this.host = Objects.requireNonNull(host);
        this.port = port;
        this.name = Objects.requireNonNull(name);
        this.user = Objects.requireNonNull(user);
        this.password = Objects.requireNonNull(password);
    }

    public Connection createConnection() throws SQLException {
        String jdbcUrl = createJDBCUrl();
        return DriverManager.getConnection(jdbcUrl, user, password);
    }

    private String createJDBCUrl()  {
        return String.format(JDBC_URL_TEMPLATE, host, port, name);
    }

}
