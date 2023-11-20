package ru.spbstu.storage.executor;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface RowReader<T> {

    T handle(ResultSet resultSet) throws SQLException;

}
