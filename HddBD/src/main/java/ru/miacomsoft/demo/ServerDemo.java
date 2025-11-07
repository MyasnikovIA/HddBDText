package ru.miacomsoft.demo;

import ru.miacomsoft.core.server.DataServer;

public class ServerDemo {
    public static void main(String[] args) {
        try {
            // Исправляем порт - должен быть в диапазоне 0-65535
            int port = 8080; // Стандартный порт для веб-серверов
            // Или можно использовать другие популярные порты:
            // 9090, 3000, 5000, 5432 (обычно для PostgreSQL), 3306 (обычно для MySQL)

            String dataFile = "storage/data.bin";
            String indexFile = "storage/index.idx";
            long memoryLimit = 500 * 1024 * 1024; // 500 MB

            System.out.println("Starting DataServer...");
            System.out.println("Port: " + port);
            System.out.println("Data file: " + dataFile);
            System.out.println("Index file: " + indexFile);
            System.out.println("Memory limit: " + (memoryLimit / (1024 * 1024)) + " MB");

            DataServer server = new DataServer(port, dataFile, indexFile, memoryLimit);

            // Добавляем shutdown hook для graceful shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\nShutting down server...");
                server.stop();
            }));

            server.start();

        } catch (Exception e) {
            System.err.println("Failed to start server: " + e.getMessage());
            e.printStackTrace();
        }
    }
}