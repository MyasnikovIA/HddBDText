package ru.miacomsoft.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SqlResult {
    private final boolean success;
    private final String message;
    private final List<Map<String, Object>> rows;
    private final int affectedRows;
    private final String[] columns;

    private SqlResult(Builder builder) {
        this.success = builder.success;
        this.message = builder.message;
        this.rows = builder.rows;
        this.affectedRows = builder.affectedRows;
        this.columns = builder.columns;
    }

    public static class Builder {
        private boolean success;
        private String message;
        private List<Map<String, Object>> rows = new ArrayList<>();
        private int affectedRows;
        private String[] columns = new String[0];

        public Builder success(boolean success) {
            this.success = success;
            return this;
        }

        public Builder message(String message) {
            this.message = message;
            return this;
        }

        public Builder addRow(Map<String, Object> row) {
            this.rows.add(new LinkedHashMap<>(row)); // сохраняем порядок
            return this;
        }

        public Builder rows(List<Map<String, Object>> rows) {
            this.rows = new ArrayList<>(rows);
            return this;
        }

        public Builder affectedRows(int count) {
            this.affectedRows = count;
            return this;
        }

        public Builder columns(String... columns) {
            this.columns = columns;
            return this;
        }

        public SqlResult build() {
            return new SqlResult(this);
        }
    }

    // Getters
    public boolean isSuccess() { return success; }
    public String getMessage() { return message; }
    public List<Map<String, Object>> getRows() { return rows; }
    public int getAffectedRows() { return affectedRows; }
    public String[] getColumns() { return columns; }

    // Вспомогательные методы
    public boolean hasRows() {
        return rows != null && !rows.isEmpty();
    }

    public int getRowCount() {
        return rows != null ? rows.size() : 0;
    }

    public Map<String, Object> getFirstRow() {
        return hasRows() ? rows.get(0) : null;
    }

    public Object getValue(int rowIndex, String column) {
        if (rows == null || rowIndex >= rows.size()) {
            return null;
        }
        return rows.get(rowIndex).get(column);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static SqlResult success(String message) {
        return new Builder().success(true).message(message).build();
    }

    public static SqlResult error(String message) {
        return new Builder().success(false).message(message).build();
    }

    @Override
    public String toString() {
        return "SqlResult{" +
                "success=" + success +
                ", message='" + message + '\'' +
                ", rowCount=" + getRowCount() +
                ", affectedRows=" + affectedRows +
                '}';
    }
}