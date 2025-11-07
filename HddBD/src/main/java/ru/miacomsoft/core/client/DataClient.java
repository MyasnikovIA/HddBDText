package ru.miacomsoft.core.client;

import ru.miacomsoft.core.SearchQuery;
import ru.miacomsoft.core.SqlQuery;
import ru.miacomsoft.core.SqlResult;
import ru.miacomsoft.core.server.protocol.*;

import java.io.*;
import java.net.Socket;
import java.util.List;
import java.util.Map;

public class DataClient implements AutoCloseable {
    private final String host;
    private final int port;
    private Socket socket;
    private DataInputStream input;
    private DataOutputStream output;

    public DataClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() throws IOException {
        socket = new Socket(host, port);
        input = new DataInputStream(socket.getInputStream());
        output = new DataOutputStream(socket.getOutputStream());
        System.out.println("Connected to server " + host + ":" + port);
    }

    private Response sendRequest(Request request) throws IOException {
        byte[] requestData = ProtocolCodec.encodeRequest(request);
        output.writeInt(requestData.length);
        output.write(requestData);
        output.flush();

        int responseLength = input.readInt();
        byte[] responseData = new byte[responseLength];
        input.readFully(responseData);

        return ProtocolCodec.decodeResponse(responseData);
    }

    public boolean put(byte[] key, byte[] value) throws IOException {
        return put(key, value, -1, null, null);
    }

    public boolean put(byte[] key, byte[] value, long expiryTime, float[] nameVector, float[] dataVector) throws IOException {
        Request request = new Request.Builder()
                .command(Command.PUT)
                .key(key)
                .value(value)
                .expiryTime(expiryTime)
                .nameVector(nameVector)
                .dataVector(dataVector)
                .build();

        Response response = sendRequest(request);
        if (!response.isSuccess()) {
            System.err.println("PUT failed: " + response.getMessage());
        }
        return response.isSuccess();
    }

    public byte[] get(byte[] key) throws IOException {
        Request request = new Request.Builder()
                .command(Command.GET)
                .key(key)
                .build();

        Response response = sendRequest(request);
        if (response.isSuccess()) {
            return response.getData();
        } else {
            System.err.println("GET failed: " + response.getMessage());
            return null;
        }
    }

    public boolean update(byte[] key, byte[] newValue) throws IOException {
        Request request = new Request.Builder()
                .command(Command.UPDATE)
                .key(key)
                .value(newValue)
                .build();

        Response response = sendRequest(request);
        if (!response.isSuccess()) {
            System.err.println("UPDATE failed: " + response.getMessage());
        }
        return response.isSuccess();
    }

    public boolean delete(byte[] key) throws IOException {
        Request request = new Request.Builder()
                .command(Command.DELETE)
                .key(key)
                .build();

        Response response = sendRequest(request);
        if (!response.isSuccess()) {
            System.err.println("DELETE failed: " + response.getMessage());
        }
        return response.isSuccess();
    }

    public List<byte[]> find(SearchQuery query) throws IOException {
        Request request = new Request.Builder()
                .command(Command.FIND)
                .searchQuery(query)
                .build();

        Response response = sendRequest(request);
        if (response.isSuccess()) {
            return response.getResults();
        } else {
            System.err.println("FIND failed: " + response.getMessage());
            return null;
        }
    }

    public Response.SystemStats getStats() throws IOException {
        Request request = new Request.Builder()
                .command(Command.STATS)
                .build();

        Response response = sendRequest(request);
        if (response.isSuccess()) {
            return response.getStats();
        } else {
            System.err.println("STATS failed: " + response.getMessage());
            return null;
        }
    }

    public boolean ping() throws IOException {
        Request request = new Request.Builder()
                .command(Command.PING)
                .build();

        Response response = sendRequest(request);
        return response.isSuccess() && "PONG".equals(response.getMessage());
    }

    @Override
    public void close() {
        try {
            if (output != null) output.close();
            if (input != null) input.close();
            if (socket != null) socket.close();
        } catch (IOException e) {
            System.err.println("Error closing client: " + e.getMessage());
        }
    }

    public boolean isConnected() {
        return socket != null && socket.isConnected() && !socket.isClosed();
    }

    // SQL методы
    public SqlResult executeSql(SqlQuery query) throws IOException {
        Request request = new Request.Builder()
                .command(Command.SQL_EXECUTE)
                .sqlQuery(query)
                .build();

        Response response = sendRequest(request);
        if (response.isSuccess()) {
            return response.getSqlResult();
        } else {
            throw new IOException("SQL execution failed: " + response.getMessage());
        }
    }

    public SqlResult querySql(SqlQuery query) throws IOException {
        Request request = new Request.Builder()
                .command(Command.SQL_QUERY)
                .sqlQuery(query)
                .build();

        Response response = sendRequest(request);
        if (response.isSuccess()) {
            return response.getSqlResult();
        } else {
            throw new IOException("SQL query failed: " + response.getMessage());
        }
    }

    // Вспомогательные методы для common SQL операций
    public SqlResult createTable(String tableName, Map<String, String> schema) throws IOException {
        SqlQuery.Builder builder = SqlQuery.builder().createTable(tableName);
        for (Map.Entry<String, String> entry : schema.entrySet()) {
            builder.column(entry.getKey(), entry.getValue());
        }
        return executeSql(builder.build());
    }

    public SqlResult dropTable(String tableName) throws IOException {
        SqlQuery query = SqlQuery.builder().dropTable(tableName).build();
        return executeSql(query);
    }

    public SqlResult insert(String tableName, Map<String, Object> values) throws IOException {
        SqlQuery.Builder builder = SqlQuery.builder().insert(tableName);
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            builder.value(entry.getKey(), entry.getValue());
        }
        return executeSql(builder.build());
    }

    public SqlResult select(String tableName, String... columns) throws IOException {
        SqlQuery query = SqlQuery.builder()
                .select(columns)
                .build();
        // Note: table name is handled differently for SELECT
        return querySql(query);
    }

    public SqlResult update(String tableName, Map<String, Object> values, Map<String, Object> whereConditions) throws IOException {
        SqlQuery.Builder builder = SqlQuery.builder().update(tableName);
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            builder.value(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, Object> entry : whereConditions.entrySet()) {
            builder.where(entry.getKey(), entry.getValue());
        }
        return executeSql(builder.build());
    }

    public SqlResult delete(String tableName, Map<String, Object> whereConditions) throws IOException {
        SqlQuery.Builder builder = SqlQuery.builder().delete(tableName);
        for (Map.Entry<String, Object> entry : whereConditions.entrySet()) {
            builder.where(entry.getKey(), entry.getValue());
        }
        return executeSql(builder.build());
    }

    public SqlResult createIndex(String tableName, String columnName) throws IOException {
        // Simplified index creation
        SqlQuery query = SqlQuery.builder()
                .createTable(tableName + "_idx_" + columnName) // Create index as separate table
                .column("value", "STRING")
                .column("row_id", "STRING")
                .build();
        return executeSql(query);
    }

    public SqlResult addRelation(String fromTable, String toTable, String relationName) throws IOException {
        // Create relation table
        SqlQuery query = SqlQuery.builder()
                .createTable(relationName)
                .column("from_id", "STRING")
                .column("to_id", "STRING")
                .column("relation_type", "STRING")
                .build();
        return executeSql(query);
    }
}