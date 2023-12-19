package ru.spbstu.storage.executor;

import org.jetbrains.annotations.NotNull;
import ru.spbstu.exception.StorageException;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DataBaseRequestExecutor {

    private final Connection connection;

    public DataBaseRequestExecutor(@NotNull Connection connection) {
        this.connection = Objects.requireNonNull(connection);
        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new StorageException("", e);
        }
    }

    public <T> Array createArray(@NotNull String type,
                                 @NotNull T[] array) throws SQLException {
        return connection.createArrayOf(type, array);
    }

    public List<Integer> executeCreate(@NotNull String updateQuery,
                                       @NotNull PreparedStatementUpdater preparedStatementUpdater) {
        Objects.requireNonNull(updateQuery);
        try (PreparedStatement prepareStatement
                     = connection.prepareStatement(updateQuery, Statement.RETURN_GENERATED_KEYS)) {
            preparedStatementUpdater.update(prepareStatement);
            prepareStatement.executeBatch();
            List<Integer> generatedIds = collectGeneratedIds(prepareStatement);
            connection.commit();
            return generatedIds;
        } catch (SQLException ex) {
            throw new StorageException(String.format("Fail to execute update, query: %s", updateQuery), ex);
        }
    }

    private List<Integer> collectGeneratedIds(@NotNull PreparedStatement preparedStatement) {
        List<Integer> generatedIds = new ArrayList<>();
        try (ResultSet generatedKeysResultSet = preparedStatement.getGeneratedKeys()) {
            while (generatedKeysResultSet.next()) {
                int metadataId = generatedKeysResultSet.getInt(1);
                generatedIds.add(metadataId);
            }
        } catch (SQLException e) {
            throw new StorageException("Failed to collect generated ids",  e);
        }
        return generatedIds;
    }


    public void executeUpdate(@NotNull String updateQuery,
                              @NotNull PreparedStatementUpdater preparedStatementUpdater) {
        Objects.requireNonNull(updateQuery);
        try (PreparedStatement prepareStatement
                     = connection.prepareStatement(updateQuery, Statement.RETURN_GENERATED_KEYS)) {
            preparedStatementUpdater.update(prepareStatement);
            prepareStatement.executeBatch();
            connection.commit();
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
            prepareStatement.executeQuery();
            try (ResultSet resultSet = prepareStatement.getResultSet()){
                return rowReader.handle(resultSet);
            }
        } catch (SQLException ex) {
            throw new StorageException(String.format("Fail to execute query, query: %s", query), ex);
        }
    }

}
