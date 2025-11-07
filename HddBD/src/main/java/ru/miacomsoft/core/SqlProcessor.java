package ru.miacomsoft.core;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SqlProcessor {
    private final BinaryDataManager dataManager;
    private final Map<String, TableSchema> tables;

    public SqlProcessor(BinaryDataManager dataManager) {
        this.dataManager = dataManager;
        this.tables = new ConcurrentHashMap<>();
        initializeSystemTables();
    }

    // Схема таблицы
    private static class TableSchema {
        String tableName;
        Map<String, String> columns; // columnName -> dataType
        String primaryKey;

        TableSchema(String tableName) {
            this.tableName = tableName;
            this.columns = new LinkedHashMap<>(); // сохраняем порядок колонок
        }
    }

    // Инициализация системных таблиц
    private void initializeSystemTables() {
        // Системная таблица с информацией о таблицах
        TableSchema tablesSchema = new TableSchema("_tables");
        tablesSchema.columns.put("table_name", "STRING");
        tablesSchema.columns.put("created_at", "LONG");
        tablesSchema.columns.put("row_count", "INT");
        tablesSchema.primaryKey = "table_name";
        tables.put("_tables", tablesSchema);
    }

    public SqlResult execute(SqlQuery query) {
        try {
            switch (query.getOperation()) {
                case SELECT:
                    return executeSelect(query);
                case INSERT:
                    return executeInsert(query);
                case UPDATE:
                    return executeUpdate(query);
                case DELETE:
                    return executeDelete(query);
                case CREATE_TABLE:
                    return executeCreateTable(query);
                case DROP_TABLE:
                    return executeDropTable(query);
                default:
                    return SqlResult.error("Unsupported operation: " + query.getOperation());
            }
        } catch (Exception e) {
            return SqlResult.error("SQL execution error: " + e.getMessage());
        }
    }

    private SqlResult executeSelect(SqlQuery query) {
        // Для SELECT без указания таблицы - возвращаем список таблиц
        if (query.getTableName() == null) {
            return listTables();
        }

        TableSchema schema = tables.get(query.getTableName());
        if (schema == null) {
            return SqlResult.error("Table not found: " + query.getTableName());
        }

        List<Map<String, Object>> results = new ArrayList<>();
        String[] columns = query.getSelectedColumns();

        // В реальной реализации здесь бы происходил поиск данных в BinaryDataManager
        // по ключам, соответствующим таблице и условиям WHERE

        // Заглушка для демонстрации
        if ("_tables".equals(query.getTableName())) {
            for (TableSchema table : tables.values()) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("table_name", table.tableName);
                row.put("created_at", System.currentTimeMillis());
                row.put("row_count", 0); // В реальной реализации нужно подсчитывать
                results.add(row);
            }
        }

        return SqlResult.builder()
                .success(true)
                .message("SELECT executed successfully")
                .rows(results)
                .columns(columns.length > 0 ? columns : schema.columns.keySet().toArray(new String[0]))
                .build();
    }

    private SqlResult executeInsert(SqlQuery query) {
        TableSchema schema = tables.get(query.getTableName());
        if (schema == null) {
            return SqlResult.error("Table not found: " + query.getTableName());
        }

        // Валидация данных
        for (String column : query.getValues().keySet()) {
            if (!schema.columns.containsKey(column)) {
                return SqlResult.error("Column not found: " + column);
            }
        }

        // В реальной реализации здесь бы происходило сохранение в BinaryDataManager
        // с ключом, включающим имя таблицы и primary key

        return SqlResult.builder()
                .success(true)
                .message("INSERT executed successfully")
                .affectedRows(1)
                .build();
    }

    private SqlResult executeUpdate(SqlQuery query) {
        TableSchema schema = tables.get(query.getTableName());
        if (schema == null) {
            return SqlResult.error("Table not found: " + query.getTableName());
        }

        // Валидация колонок для обновления
        for (String column : query.getValues().keySet()) {
            if (!schema.columns.containsKey(column)) {
                return SqlResult.error("Column not found: " + column);
            }
        }

        // В реальной реализации здесь бы происходило обновление в BinaryDataManager
        // с поиском по условиям WHERE

        return SqlResult.builder()
                .success(true)
                .message("UPDATE executed successfully")
                .affectedRows(1) // В реальной реализации подсчитывались бы реальные обновленные строки
                .build();
    }

    private SqlResult executeDelete(SqlQuery query) {
        TableSchema schema = tables.get(query.getTableName());
        if (schema == null) {
            return SqlResult.error("Table not found: " + query.getTableName());
        }

        // В реальной реализации здесь бы происходило удаление из BinaryDataManager
        // с поиском по условиям WHERE

        return SqlResult.builder()
                .success(true)
                .message("DELETE executed successfully")
                .affectedRows(1) // В реальной реализации подсчитывались бы реальные удаленные строки
                .build();
    }

    private SqlResult executeCreateTable(SqlQuery query) {
        if (tables.containsKey(query.getTableName())) {
            return SqlResult.error("Table already exists: " + query.getTableName());
        }

        TableSchema schema = new TableSchema(query.getTableName());
        schema.columns.putAll(query.getSchema());

        // Добавляем системные колонки
        schema.columns.put("_id", "STRING");
        schema.columns.put("_created_at", "LONG");
        schema.columns.put("_updated_at", "LONG");

        tables.put(query.getTableName(), schema);

        return SqlResult.builder()
                .success(true)
                .message("Table created successfully: " + query.getTableName())
                .affectedRows(0)
                .build();
    }

    private SqlResult executeDropTable(SqlQuery query) {
        if (!tables.containsKey(query.getTableName())) {
            return SqlResult.error("Table not found: " + query.getTableName());
        }

        tables.remove(query.getTableName());

        // В реальной реализации здесь бы происходило удаление всех данных таблицы из BinaryDataManager

        return SqlResult.builder()
                .success(true)
                .message("Table dropped successfully: " + query.getTableName())
                .affectedRows(0)
                .build();
    }

    private SqlResult listTables() {
        List<Map<String, Object>> tablesList = new ArrayList<>();

        for (String tableName : tables.keySet()) {
            if (!tableName.startsWith("_")) { // Показываем только пользовательские таблицы
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("table_name", tableName);
                tablesList.add(row);
            }
        }

        return SqlResult.builder()
                .success(true)
                .message("Tables list retrieved")
                .rows(tablesList)
                .columns(new String[]{"table_name"})
                .build();
    }

    // Вспомогательные методы
    public boolean tableExists(String tableName) {
        return tables.containsKey(tableName);
    }

    public List<String> getTableNames() {
        return new ArrayList<>(tables.keySet());
    }

    public Map<String, String> getTableSchema(String tableName) {
        TableSchema schema = tables.get(tableName);
        return schema != null ? new LinkedHashMap<>(schema.columns) : null;
    }
}