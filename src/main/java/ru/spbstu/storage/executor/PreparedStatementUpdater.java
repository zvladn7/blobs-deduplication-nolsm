package ru.spbstu.storage.executor;

import org.jetbrains.annotations.NotNull;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface PreparedStatementUpdater {

    void update(@NotNull PreparedStatement preparedStatement) throws SQLException;

}
