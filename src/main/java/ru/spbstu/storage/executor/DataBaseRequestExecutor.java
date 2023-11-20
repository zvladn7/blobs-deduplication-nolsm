package ru.spbstu.storage.executor;

import org.jetbrains.annotations.NotNull;
import ru.spbstu.exception.StorageException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

public class DataBaseRequestExecutor {

    private final Connection connection;

    public DataBaseRequestExecutor(@NotNull Connection connection) {
        this.connection = Objects.requireNonNull(connection);
    }

    public int executeUpdate(@NotNull String updateQuery,
                             @NotNull PreparedStatementUpdater preparedStatementUpdater) {
        Objects.requireNonNull(updateQuery);
        try (PreparedStatement prepareStatement = connection.prepareStatement(updateQuery)) {
            preparedStatementUpdater.update(prepareStatement);
            return prepareStatement.executeUpdate(updateQuery);
        } catch (SQLException ex) {
            throw new StorageException(String.format("Fail to execute update, query: %s", updateQuery), ex);
        }
    }

    @NotNull
    public <T> T executeQuery(@NotNull String query,
                              @NotNull PreparedStatementUpdater preparedStatementUpdater,
                              @NotNull RowReader<T> rowReader) {
        Objects.requireNonNull(query);
        Objects.requireNonNull(rowReader);
        try (PreparedStatement prepareStatement = connection.prepareStatement(query)) {
            preparedStatementUpdater.update(prepareStatement);
            prepareStatement.executeQuery(query);
            try (ResultSet resultSet = prepareStatement.getResultSet()){
                return rowReader.handle(resultSet);
            }
        } catch (SQLException ex) {
            throw new StorageException(String.format("Fail to execute query, query: %s", query), ex);
        }
    }

}
