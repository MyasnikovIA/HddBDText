package ru.miacomsoft.core.server.protocol;

import ru.miacomsoft.core.SearchQuery;
import ru.miacomsoft.core.SqlQuery;
import ru.miacomsoft.core.SqlResult;

import java.io.*;
import java.util.*;

public class ProtocolCodec {

    public static byte[] encodeRequest(Request request) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        // Command
        dos.writeInt(request.getCommand().getCode());

        // Key
        if (request.getKey() != null) {
            dos.writeInt(request.getKey().length);
            dos.write(request.getKey());
        } else {
            dos.writeInt(0);
        }

        // Value
        if (request.getValue() != null) {
            dos.writeInt(request.getValue().length);
            dos.write(request.getValue());
        } else {
            dos.writeInt(0);
        }

        // SearchQuery
        if (request.getSearchQuery() != null) {
            dos.writeBoolean(true);
            encodeSearchQuery(dos, request.getSearchQuery());
        } else {
            dos.writeBoolean(false);
        }

        // Expiry time
        dos.writeLong(request.getExpiryTime());

        // Name vector
        if (request.getNameVector() != null) {
            dos.writeInt(request.getNameVector().length);
            for (float f : request.getNameVector()) {
                dos.writeFloat(f);
            }
        } else {
            dos.writeInt(0);
        }

        // Data vector
        if (request.getDataVector() != null) {
            dos.writeInt(request.getDataVector().length);
            for (float f : request.getDataVector()) {
                dos.writeFloat(f);
            }
        } else {
            dos.writeInt(0);
        }

        // SQL Query
        if (request.getSqlQuery() != null) {
            dos.writeBoolean(true);
            encodeSqlQuery(dos, request.getSqlQuery());
        } else {
            dos.writeBoolean(false);
        }

        dos.flush();
        return baos.toByteArray();
    }

    public static Request decodeRequest(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);

        Command command = Command.fromCode(dis.readInt());

        // Key
        int keyLength = dis.readInt();
        byte[] key = null;
        if (keyLength > 0) {
            key = new byte[keyLength];
            dis.readFully(key);
        }

        // Value
        int valueLength = dis.readInt();
        byte[] value = null;
        if (valueLength > 0) {
            value = new byte[valueLength];
            dis.readFully(value);
        }

        // SearchQuery
        SearchQuery searchQuery = null;
        if (dis.readBoolean()) {
            searchQuery = decodeSearchQuery(dis);
        }

        long expiryTime = dis.readLong();

        // Name vector
        int nameVectorLength = dis.readInt();
        float[] nameVector = null;
        if (nameVectorLength > 0) {
            nameVector = new float[nameVectorLength];
            for (int i = 0; i < nameVectorLength; i++) {
                nameVector[i] = dis.readFloat();
            }
        }

        // Data vector
        int dataVectorLength = dis.readInt();
        float[] dataVector = null;
        if (dataVectorLength > 0) {
            dataVector = new float[dataVectorLength];
            for (int i = 0; i < dataVectorLength; i++) {
                dataVector[i] = dis.readFloat();
            }
        }

        // SQL Query
        SqlQuery sqlQuery = null;
        if (dis.readBoolean()) {
            sqlQuery = decodeSqlQuery(dis);
        }

        return new Request.Builder()
                .command(command)
                .key(key)
                .value(value)
                .searchQuery(searchQuery)
                .expiryTime(expiryTime)
                .nameVector(nameVector)
                .dataVector(dataVector)
                .sqlQuery(sqlQuery)
                .build();
    }

    public static byte[] encodeResponse(Response response) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        // Success flag
        dos.writeBoolean(response.isSuccess());

        // Message
        if (response.getMessage() != null) {
            byte[] messageBytes = response.getMessage().getBytes("UTF-8");
            dos.writeInt(messageBytes.length);
            dos.write(messageBytes);
        } else {
            dos.writeInt(0);
        }

        // Data
        if (response.getData() != null) {
            dos.writeInt(response.getData().length);
            dos.write(response.getData());
        } else {
            dos.writeInt(0);
        }

        // Results
        if (response.getResults() != null) {
            dos.writeInt(response.getResults().size());
            for (byte[] result : response.getResults()) {
                dos.writeInt(result.length);
                dos.write(result);
            }
        } else {
            dos.writeInt(0);
        }

        // Stats
        if (response.getStats() != null) {
            dos.writeBoolean(true);
            Response.SystemStats stats = response.getStats();
            dos.writeInt(stats.getIndexSize());
            dos.writeLong(stats.getDataFileSize());
            dos.writeInt(stats.getFreeSpaceBlocks());
            dos.writeLong(stats.getTotalFreeSpace());
            dos.writeLong(stats.getUsedMemory());
            dos.writeLong(stats.getMaxMemory());
            dos.writeDouble(stats.getMemoryUsageRatio());
            dos.writeInt(stats.getCacheSize());
        } else {
            dos.writeBoolean(false);
        }

        // SQL Result
        if (response.getSqlResult() != null) {
            dos.writeBoolean(true);
            encodeSqlResult(dos, response.getSqlResult());
        } else {
            dos.writeBoolean(false);
        }

        dos.flush();
        return baos.toByteArray();
    }

    public static Response decodeResponse(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);

        boolean success = dis.readBoolean();

        // Message
        int messageLength = dis.readInt();
        String message = null;
        if (messageLength > 0) {
            byte[] messageBytes = new byte[messageLength];
            dis.readFully(messageBytes);
            message = new String(messageBytes, "UTF-8");
        }

        // Data
        int dataLength = dis.readInt();
        byte[] responseData = null;
        if (dataLength > 0) {
            responseData = new byte[dataLength];
            dis.readFully(responseData);
        }

        // Results
        int resultsCount = dis.readInt();
        List<byte[]> results = null;
        if (resultsCount > 0) {
            results = new ArrayList<>();
            for (int i = 0; i < resultsCount; i++) {
                int resultLength = dis.readInt();
                byte[] result = new byte[resultLength];
                dis.readFully(result);
                results.add(result);
            }
        }

        // Stats
        Response.SystemStats stats = null;
        if (dis.readBoolean()) {
            int indexSize = dis.readInt();
            long dataFileSize = dis.readLong();
            int freeSpaceBlocks = dis.readInt();
            long totalFreeSpace = dis.readLong();
            long usedMemory = dis.readLong();
            long maxMemory = dis.readLong();
            double memoryUsageRatio = dis.readDouble();
            int cacheSize = dis.readInt();

            stats = new Response.SystemStats(
                    indexSize,
                    dataFileSize,
                    freeSpaceBlocks,
                    totalFreeSpace,
                    usedMemory,
                    maxMemory,
                    memoryUsageRatio,
                    cacheSize
            );
        }

        // SQL Result
        SqlResult sqlResult = null;
        if (dis.readBoolean()) {
            sqlResult = decodeSqlResult(dis);
        }

        return new Response(success, message, responseData, results, stats, sqlResult);
    }

    // Методы для работы с SqlQuery
    private static void encodeSqlQuery(DataOutputStream dos, SqlQuery query) throws IOException {
        dos.writeInt(query.getOperation().ordinal());

        // Table name
        if (query.getTableName() != null) {
            byte[] tableNameBytes = query.getTableName().getBytes("UTF-8");
            dos.writeInt(tableNameBytes.length);
            dos.write(tableNameBytes);
        } else {
            dos.writeInt(0);
        }

        // Values
        Map<String, Object> values = query.getValues();
        dos.writeInt(values.size());
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            // Key
            byte[] keyBytes = entry.getKey().getBytes("UTF-8");
            dos.writeInt(keyBytes.length);
            dos.write(keyBytes);

            // Value
            encodeObject(dos, entry.getValue());
        }

        // Where conditions
        Map<String, Object> whereConditions = query.getWhereConditions();
        dos.writeInt(whereConditions.size());
        for (Map.Entry<String, Object> entry : whereConditions.entrySet()) {
            // Key
            byte[] keyBytes = entry.getKey().getBytes("UTF-8");
            dos.writeInt(keyBytes.length);
            dos.write(keyBytes);

            // Value
            encodeObject(dos, entry.getValue());
        }

        // Selected columns
        String[] selectedColumns = query.getSelectedColumns();
        dos.writeInt(selectedColumns.length);
        for (String column : selectedColumns) {
            byte[] columnBytes = column.getBytes("UTF-8");
            dos.writeInt(columnBytes.length);
            dos.write(columnBytes);
        }

        // Schema
        Map<String, String> schema = query.getSchema();
        dos.writeInt(schema.size());
        for (Map.Entry<String, String> entry : schema.entrySet()) {
            // Column name
            byte[] columnBytes = entry.getKey().getBytes("UTF-8");
            dos.writeInt(columnBytes.length);
            dos.write(columnBytes);

            // Data type
            byte[] typeBytes = entry.getValue().getBytes("UTF-8");
            dos.writeInt(typeBytes.length);
            dos.write(typeBytes);
        }
    }

    private static SqlQuery decodeSqlQuery(DataInputStream dis) throws IOException {
        SqlQuery.Operation operation = SqlQuery.Operation.values()[dis.readInt()];
        SqlQuery.Builder builder = new SqlQuery.Builder();

        // Table name
        int tableNameLength = dis.readInt();
        String tableName = null;
        if (tableNameLength > 0) {
            byte[] tableNameBytes = new byte[tableNameLength];
            dis.readFully(tableNameBytes);
            tableName = new String(tableNameBytes, "UTF-8");
        }

        // Set operation and table name
        switch (operation) {
            case SELECT:
                builder.select(); // columns will be set later
                break;
            case INSERT:
                builder.insert(tableName);
                break;
            case UPDATE:
                builder.update(tableName);
                break;
            case DELETE:
                builder.delete(tableName);
                break;
            case CREATE_TABLE:
                builder.createTable(tableName);
                break;
            case DROP_TABLE:
                builder.dropTable(tableName);
                break;
        }

        // Values
        int valuesCount = dis.readInt();
        for (int i = 0; i < valuesCount; i++) {
            int keyLength = dis.readInt();
            byte[] keyBytes = new byte[keyLength];
            dis.readFully(keyBytes);
            String key = new String(keyBytes, "UTF-8");

            Object value = decodeObject(dis);
            builder.value(key, value);
        }

        // Where conditions
        int whereCount = dis.readInt();
        for (int i = 0; i < whereCount; i++) {
            int keyLength = dis.readInt();
            byte[] keyBytes = new byte[keyLength];
            dis.readFully(keyBytes);
            String key = new String(keyBytes, "UTF-8");

            Object value = decodeObject(dis);
            builder.where(key, value);
        }

        // Selected columns
        int columnsCount = dis.readInt();
        String[] selectedColumns = new String[columnsCount];
        for (int i = 0; i < columnsCount; i++) {
            int columnLength = dis.readInt();
            byte[] columnBytes = new byte[columnLength];
            dis.readFully(columnBytes);
            selectedColumns[i] = new String(columnBytes, "UTF-8");
        }

        // Set selected columns for SELECT operation
        if (operation == SqlQuery.Operation.SELECT && columnsCount > 0) {
            builder.select(selectedColumns);
        }

        // Schema
        int schemaCount = dis.readInt();
        for (int i = 0; i < schemaCount; i++) {
            int columnLength = dis.readInt();
            byte[] columnBytes = new byte[columnLength];
            dis.readFully(columnBytes);
            String column = new String(columnBytes, "UTF-8");

            int typeLength = dis.readInt();
            byte[] typeBytes = new byte[typeLength];
            dis.readFully(typeBytes);
            String dataType = new String(typeBytes, "UTF-8");

            builder.column(column, dataType);
        }

        return builder.build();
    }

    // Методы для работы с SqlResult
    private static void encodeSqlResult(DataOutputStream dos, SqlResult result) throws IOException {
        dos.writeBoolean(result.isSuccess());

        // Message
        if (result.getMessage() != null) {
            byte[] messageBytes = result.getMessage().getBytes("UTF-8");
            dos.writeInt(messageBytes.length);
            dos.write(messageBytes);
        } else {
            dos.writeInt(0);
        }

        // Rows
        List<Map<String, Object>> rows = result.getRows();
        dos.writeInt(rows.size());
        for (Map<String, Object> row : rows) {
            dos.writeInt(row.size());
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                // Key
                byte[] keyBytes = entry.getKey().getBytes("UTF-8");
                dos.writeInt(keyBytes.length);
                dos.write(keyBytes);

                // Value
                encodeObject(dos, entry.getValue());
            }
        }

        // Affected rows
        dos.writeInt(result.getAffectedRows());

        // Columns
        String[] columns = result.getColumns();
        dos.writeInt(columns.length);
        for (String column : columns) {
            byte[] columnBytes = column.getBytes("UTF-8");
            dos.writeInt(columnBytes.length);
            dos.write(columnBytes);
        }
    }

    private static SqlResult decodeSqlResult(DataInputStream dis) throws IOException {
        boolean success = dis.readBoolean();

        // Message
        int messageLength = dis.readInt();
        String message = null;
        if (messageLength > 0) {
            byte[] messageBytes = new byte[messageLength];
            dis.readFully(messageBytes);
            message = new String(messageBytes, "UTF-8");
        }

        SqlResult.Builder builder = SqlResult.builder()
                .success(success)
                .message(message);

        // Rows
        int rowsCount = dis.readInt();
        for (int i = 0; i < rowsCount; i++) {
            Map<String, Object> row = new LinkedHashMap<>();
            int rowSize = dis.readInt();
            for (int j = 0; j < rowSize; j++) {
                int keyLength = dis.readInt();
                byte[] keyBytes = new byte[keyLength];
                dis.readFully(keyBytes);
                String key = new String(keyBytes, "UTF-8");

                Object value = decodeObject(dis);
                row.put(key, value);
            }
            builder.addRow(row);
        }

        // Affected rows
        int affectedRows = dis.readInt();
        builder.affectedRows(affectedRows);

        // Columns
        int columnsCount = dis.readInt();
        String[] columns = new String[columnsCount];
        for (int i = 0; i < columnsCount; i++) {
            int columnLength = dis.readInt();
            byte[] columnBytes = new byte[columnLength];
            dis.readFully(columnBytes);
            columns[i] = new String(columnBytes, "UTF-8");
        }
        builder.columns(columns);

        return builder.build();
    }

    // Вспомогательные методы для кодирования/декодирования объектов
    private static void encodeObject(DataOutputStream dos, Object obj) throws IOException {
        if (obj == null) {
            dos.writeInt(0); // NULL type
        } else if (obj instanceof String) {
            dos.writeInt(1); // STRING type
            byte[] strBytes = ((String) obj).getBytes("UTF-8");
            dos.writeInt(strBytes.length);
            dos.write(strBytes);
        } else if (obj instanceof Integer) {
            dos.writeInt(2); // INT type
            dos.writeInt((Integer) obj);
        } else if (obj instanceof Long) {
            dos.writeInt(3); // LONG type
            dos.writeLong((Long) obj);
        } else if (obj instanceof Double) {
            dos.writeInt(4); // DOUBLE type
            dos.writeDouble((Double) obj);
        } else if (obj instanceof Boolean) {
            dos.writeInt(5); // BOOLEAN type
            dos.writeBoolean((Boolean) obj);
        } else {
            throw new IOException("Unsupported object type: " + obj.getClass().getName());
        }
    }

    private static Object decodeObject(DataInputStream dis) throws IOException {
        int type = dis.readInt();
        switch (type) {
            case 0: // NULL
                return null;
            case 1: // STRING
                int strLength = dis.readInt();
                byte[] strBytes = new byte[strLength];
                dis.readFully(strBytes);
                return new String(strBytes, "UTF-8");
            case 2: // INT
                return dis.readInt();
            case 3: // LONG
                return dis.readLong();
            case 4: // DOUBLE
                return dis.readDouble();
            case 5: // BOOLEAN
                return dis.readBoolean();
            default:
                throw new IOException("Unknown object type: " + type);
        }
    }

    // Существующие методы для SearchQuery (оставляем без изменений)
    private static void encodeSearchQuery(DataOutputStream dos, SearchQuery query) throws IOException {
        dos.writeInt(query.getType().ordinal());

        if (query.getKey() != null) {
            dos.writeInt(query.getKey().length);
            dos.write(query.getKey());
        } else {
            dos.writeInt(0);
        }

        if (query.getMask() != null) {
            dos.writeInt(query.getMask().length);
            dos.write(query.getMask());
        } else {
            dos.writeInt(0);
        }

        if (query.getVector() != null) {
            dos.writeInt(query.getVector().length);
            for (float f : query.getVector()) {
                dos.writeFloat(f);
            }
        } else {
            dos.writeInt(0);
        }

        dos.writeDouble(query.getSimilarityThreshold());

        if (query.getSearchNode() != null) {
            dos.writeInt(query.getSearchNode().length);
            dos.write(query.getSearchNode());
        } else {
            dos.writeInt(0);
        }
    }

    private static SearchQuery decodeSearchQuery(DataInputStream dis) throws IOException {
        SearchQuery.SearchType type = SearchQuery.SearchType.values()[dis.readInt()];

        SearchQuery.Builder builder = new SearchQuery.Builder();

        int keyLength = dis.readInt();
        if (keyLength > 0) {
            byte[] key = new byte[keyLength];
            dis.readFully(key);
            builder.exactMatch(key);
        }

        int maskLength = dis.readInt();
        if (maskLength > 0) {
            byte[] mask = new byte[maskLength];
            dis.readFully(mask);
            builder.maskSearch(mask);
        }

        int vectorLength = dis.readInt();
        if (vectorLength > 0) {
            float[] vector = new float[vectorLength];
            for (int i = 0; i < vectorLength; i++) {
                vector[i] = dis.readFloat();
            }
            double threshold = dis.readDouble();
            builder.vectorSearch(vector, threshold);
        } else {
            dis.readDouble(); // skip threshold
        }

        int nodeLength = dis.readInt();
        if (nodeLength > 0) {
            byte[] node = new byte[nodeLength];
            dis.readFully(node);
            builder.withSearchNode(node);
        }

        return builder.build();
    }
}