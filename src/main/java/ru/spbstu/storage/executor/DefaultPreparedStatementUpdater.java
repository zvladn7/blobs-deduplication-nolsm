package ru.spbstu.storage.executor;

import org.jetbrains.annotations.NotNull;

import java.sql.PreparedStatement;

public class DefaultPreparedStatementUpdater implements PreparedStatementUpdater {

    public static final DefaultPreparedStatementUpdater INSTANCE = new DefaultPreparedStatementUpdater();

    private DefaultPreparedStatementUpdater() {
    }

    @Override
    public void update(@NotNull PreparedStatement preparedStatement) {
    }
}
