package ru.miacomsoft.core.server;

import ru.miacomsoft.core.*;
import ru.miacomsoft.core.server.protocol.*;

import java.io.*;
import java.net.Socket;
import java.util.List;

public class ClientHandler implements Runnable {
    private final Socket clientSocket;
    private final BinaryDataManager dataManager;
    private final MemoryManager memoryManager;

    public ClientHandler(Socket socket, BinaryDataManager dataManager, MemoryManager memoryManager) {
        this.clientSocket = socket;
        this.dataManager = dataManager;
        this.memoryManager = memoryManager;
    }

    @Override
    public void run() {
        try (InputStream input = clientSocket.getInputStream();
             OutputStream output = clientSocket.getOutputStream()) {

            System.out.println("Client connected: " + clientSocket.getRemoteSocketAddress());

            while (!clientSocket.isClosed()) {
                // Читаем длину запроса
                DataInputStream dis = new DataInputStream(input);
                int requestLength = dis.readInt();
                if (requestLength <= 0) {
                    break; // Клиент отключился
                }

                // Читаем данные запроса
                byte[] requestData = new byte[requestLength];
                dis.readFully(requestData);

                // Обрабатываем запрос
                byte[] responseData = processRequest(requestData);

                // Отправляем ответ
                DataOutputStream dos = new DataOutputStream(output);
                dos.writeInt(responseData.length);
                dos.write(responseData);
                dos.flush();
            }

        } catch (IOException e) {
            System.err.println("Client handler error: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.err.println("Error closing client socket: " + e.getMessage());
            }
            System.out.println("Client disconnected: " + clientSocket.getRemoteSocketAddress());
        }
    }

    private byte[] processRequest(byte[] requestData) {
        try {
            Request request = ProtocolCodec.decodeRequest(requestData);
            System.out.println("Processing request: " + request);

            Response response = handleCommand(request);
            return ProtocolCodec.encodeResponse(response);

        } catch (Exception e) {
            System.err.println("Error processing request: " + e.getMessage());
            try {
                return ProtocolCodec.encodeResponse(new Response(false, "Error: " + e.getMessage()));
            } catch (IOException ex) {
                return new byte[0]; // Fallback
            }
        }
    }

    private Response handleCommand(Request request) {
        try {
            switch (request.getCommand()) {
                case PUT:
                    // Выделяем память под операцию
                    long memoryNeeded = estimateMemoryUsage(request);
                    if (!memoryManager.allocateMemory(memoryNeeded)) {
                        return new Response(false, "Memory limit exceeded");
                    }

                    try {
                        dataManager.put(request.getKey(), request.getValue(),
                                request.getExpiryTime(), request.getNameVector(),
                                request.getDataVector());
                        return new Response(true, "Data stored successfully");
                    } finally {
                        memoryManager.releaseMemory(memoryNeeded);
                    }

                case GET:
                    byte[] data = dataManager.get(request.getKey());
                    if (data != null) {
                        return new Response(true, "Data retrieved successfully", data);
                    } else {
                        return new Response(false, "Key not found or expired");
                    }

                case UPDATE:
                    memoryNeeded = estimateMemoryUsage(request);
                    if (!memoryManager.allocateMemory(memoryNeeded)) {
                        return new Response(false, "Memory limit exceeded");
                    }

                    try {
                        dataManager.update(request.getKey(), request.getValue());
                        return new Response(true, "Data updated successfully");
                    } finally {
                        memoryManager.releaseMemory(memoryNeeded);
                    }

                case DELETE:
                    dataManager.delete(request.getKey());
                    return new Response(true, "Data deleted successfully");

                case FIND:
                    List<byte[]> results = dataManager.find(request.getSearchQuery());
                    return new Response(true, "Search completed", results);

                case PING:
                    return new Response(true, "PONG");

                case STATS:
                    // Исправлено: убрано дублирование переменной stats
                    Response.SystemStats systemStats = new Response.SystemStats(
                            dataManager.getIndexSize(),
                            dataManager.getDataFileSize(),
                            dataManager.getFreeSpaceManager().getFreeSpaceCount(),
                            dataManager.getFreeSpaceManager().getTotalFreeSpace(),
                            memoryManager.getUsedMemory(),
                            memoryManager.getMaxMemory(),
                            memoryManager.getMemoryUsageRatio(),
                            dataManager.getCache().getSize()
                    );
                    return new Response(true, "System statistics", systemStats);

                // SQL команды
                case SQL_EXECUTE:
                case SQL_QUERY:
                case SQL_CREATE_TABLE:
                case SQL_DROP_TABLE:
                case SQL_INSERT:
                case SQL_SELECT:
                case SQL_UPDATE:
                case SQL_DELETE:
                case SQL_CREATE_INDEX:
                case SQL_ADD_RELATION:
                    return handleSqlCommand(request);

                default:
                    return new Response(false, "Unknown command");
            }
        } catch (Exception e) {
            return new Response(false, "Error: " + e.getMessage());
        }
    }

    private Response handleSqlCommand(Request request) {
        try {
            SqlQuery sqlQuery = request.getSqlQuery();
            if (sqlQuery == null) {
                return new Response(false, "SQL query is required for SQL commands");
            }

            SqlResult result = dataManager.executeSql(sqlQuery);
            return new Response(result.isSuccess(), result.getMessage(), result);

        } catch (Exception e) {
            return new Response(false, "SQL execution error: " + e.getMessage());
        }
    }

    private long estimateMemoryUsage(Request request) {
        long usage = 0;
        if (request.getKey() != null) usage += request.getKey().length;
        if (request.getValue() != null) usage += request.getValue().length;
        if (request.getNameVector() != null) usage += request.getNameVector().length * 4;
        if (request.getDataVector() != null) usage += request.getDataVector().length * 4;
        return usage + 1024; // +1KB для служебных структур
    }
}