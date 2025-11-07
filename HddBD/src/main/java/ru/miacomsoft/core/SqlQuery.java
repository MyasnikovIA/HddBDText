package ru.miacomsoft.core;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SqlQuery {
    public enum Operation {
        SELECT,
        INSERT,
        UPDATE,
        DELETE,
        CREATE_TABLE,
        DROP_TABLE
    }

    private final Operation operation;
    private final String tableName;
    private final Map<String, Object> values;
    private final Map<String, Object> whereConditions;
    private final String[] selectedColumns;
    private final Map<String, String> schema;

    private SqlQuery(Builder builder) {
        this.operation = builder.operation;
        this.tableName = builder.tableName;
        this.values = builder.values;
        this.whereConditions = builder.whereConditions;
        this.selectedColumns = builder.selectedColumns;
        this.schema = builder.schema;
    }

    public static class Builder {
        private Operation operation;
        private String tableName;
        private Map<String, Object> values = new HashMap<>();
        private Map<String, Object> whereConditions = new HashMap<>();
        private String[] selectedColumns = new String[0];
        private Map<String, String> schema = new HashMap<>();

        public Builder select(String... columns) {
            this.operation = Operation.SELECT;
            this.selectedColumns = columns != null ? columns : new String[]{"*"};
            return this;
        }

        public Builder insert(String tableName) {
            this.operation = Operation.INSERT;
            this.tableName = tableName;
            return this;
        }

        public Builder update(String tableName) {
            this.operation = Operation.UPDATE;
            this.tableName = tableName;
            return this;
        }

        public Builder delete(String tableName) {
            this.operation = Operation.DELETE;
            this.tableName = tableName;
            return this;
        }

        public Builder createTable(String tableName) {
            this.operation = Operation.CREATE_TABLE;
            this.tableName = tableName;
            return this;
        }

        public Builder dropTable(String tableName) {
            this.operation = Operation.DROP_TABLE;
            this.tableName = tableName;
            return this;
        }

        public Builder value(String column, Object value) {
            this.values.put(column, value);
            return this;
        }

        public Builder values(Map<String, Object> values) {
            this.values.putAll(values);
            return this;
        }

        public Builder where(String column, Object value) {
            this.whereConditions.put(column, value);
            return this;
        }

        public Builder where(Map<String, Object> conditions) {
            this.whereConditions.putAll(conditions);
            return this;
        }

        public Builder column(String columnName, String dataType) {
            this.schema.put(columnName, dataType);
            return this;
        }

        public Builder columns(Map<String, String> schema) {
            this.schema.putAll(schema);
            return this;
        }

        public SqlQuery build() {
            if (operation == null) {
                throw new IllegalStateException("SQL operation must be specified");
            }
            if (tableName == null && operation != Operation.SELECT) {
                throw new IllegalStateException("Table name must be specified for " + operation);
            }
            return new SqlQuery(this);
        }
    }

    // Getters
    public Operation getOperation() { return operation; }
    public String getTableName() { return tableName; }
    public Map<String, Object> getValues() { return values; }
    public Map<String, Object> getWhereConditions() { return whereConditions; }
    public String[] getSelectedColumns() { return selectedColumns; }
    public Map<String, String> getSchema() { return schema; }

    @Override
    public String toString() {
        return "SqlQuery{" +
                "operation=" + operation +
                ", tableName='" + tableName + '\'' +
                ", values=" + values +
                ", whereConditions=" + whereConditions +
                ", selectedColumns=" + Arrays.toString(selectedColumns) +
                ", schema=" + schema +
                '}';
    }

    // Вспомогательные методы для построения запросов
    public static Builder builder() {
        return new Builder();
    }

    public static SqlQuery select(String... columns) {
        return new Builder().select(columns).build();
    }

    public static SqlQuery selectFrom(String table, String... columns) {
        return new Builder().select(columns).build();
    }

    public static SqlQuery insertInto(String table) {
        return new Builder().insert(table).build();
    }

    public static SqlQuery update(String table) {
        return new Builder().update(table).build();
    }

    public static SqlQuery deleteFrom(String table) {
        return new Builder().delete(table).build();
    }

    public static SqlQuery createTable(String table) {
        return new Builder().createTable(table).build();
    }

    public static SqlQuery dropTable(String table) {
        return new Builder().dropTable(table).build();
    }
}