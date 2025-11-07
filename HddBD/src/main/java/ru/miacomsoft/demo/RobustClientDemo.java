package ru.miacomsoft.demo;

import ru.miacomsoft.core.client.DataClient;


public class RobustClientDemo {
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_DELAY_MS = 1000;

    public static void main(String[] args) {
        System.out.println("=== Robust Client Demo ===");

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try (DataClient client = new DataClient("localhost", 8080)) {

                System.out.println("Attempt " + attempt + " to connect...");
                client.connect();

                if (client.isConnected()) {
                    System.out.println("✓ Successfully connected to server");
                    runDemoOperations(client);
                    break;
                }

            } catch (Exception e) {
                System.err.println("Attempt " + attempt + " failed: " + e.getMessage());

                if (attempt < MAX_RETRIES) {
                    System.out.println("Retrying in " + RETRY_DELAY_MS + "ms...");
                    try {
                        Thread.sleep(RETRY_DELAY_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                } else {
                    System.err.println("All connection attempts failed");
                }
            }
        }
    }

    private static void runDemoOperations(DataClient client) throws Exception {
        // Тестируем различные сценарии

        // Сценарий 1: Нормальные операции
        System.out.println("\n--- Scenario 1: Normal Operations ---");
        testNormalOperations(client);

        // Сценарий 2: Обработка отсутствующих данных
        System.out.println("\n--- Scenario 2: Missing Data Handling ---");
        testMissingDataHandling(client);

        // Сценарий 3: Большие данные
        System.out.println("\n--- Scenario 3: Large Data ---");
        testLargeData(client);

        // Сценарий 4: Многопоточная симуляция
        System.out.println("\n--- Scenario 4: Concurrent Operations ---");
        testConcurrentOperations(client);

        System.out.println("\n=== Robust Demo Completed ===");
    }

    private static void testNormalOperations(DataClient client) throws Exception {
        String key = "test:normal:1";
        String value = "Normal operation test data";

        // Запись
        if (client.put(key.getBytes(), value.getBytes())) {
            System.out.println("✓ Write operation successful");
        }

        // Чтение
        byte[] result = client.get(key.getBytes());
        if (result != null && new String(result).equals(value)) {
            System.out.println("✓ Read operation successful");
        }

        // Обновление
        String updatedValue = "Updated normal operation test data";
        if (client.update(key.getBytes(), updatedValue.getBytes())) {
            System.out.println("✓ Update operation successful");
        }

        // Удаление
        if (client.delete(key.getBytes())) {
            System.out.println("✓ Delete operation successful");
        }
    }

    private static void testMissingDataHandling(DataClient client) throws Exception {
        String nonExistentKey = "test:non:existent";

        // Попытка чтения несуществующих данных
        byte[] result = client.get(nonExistentKey.getBytes());
        if (result == null) {
            System.out.println("✓ Correctly handled missing data (returned null)");
        }

        // Попытка обновления несуществующих данных
        try {
            client.update(nonExistentKey.getBytes(), "some data".getBytes());
            System.err.println("✗ Expected exception for updating non-existent key");
        } catch (Exception e) {
            System.out.println("✓ Correctly threw exception for updating non-existent key: " + e.getMessage());
        }

        // Удаление несуществующих данных (должно работать без ошибок)
        if (client.delete(nonExistentKey.getBytes())) {
            System.out.println("✓ Delete operation on non-existent key completed without error");
        }
    }

    private static void testLargeData(DataClient client) throws Exception {
        // Создаем большой набор данных (100KB)
        StringBuilder largeDataBuilder = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            largeDataBuilder.append("Line ").append(i).append(": This is a large data test.\n");
        }
        String largeData = largeDataBuilder.toString();

        String largeKey = "test:large:data";

        if (client.put(largeKey.getBytes(), largeData.getBytes())) {
            System.out.println("✓ Successfully stored large data (" + largeData.getBytes().length + " bytes)");
        }

        byte[] retrievedData = client.get(largeKey.getBytes());
        if (retrievedData != null && retrievedData.length == largeData.getBytes().length) {
            System.out.println("✓ Successfully retrieved large data (" + retrievedData.length + " bytes)");
        }

        // Очистка
        client.delete(largeKey.getBytes());
    }

    private static void testConcurrentOperations(DataClient client) throws Exception {
        int threadCount = 5;
        Thread[] threads = new Thread[threadCount];

        System.out.println("Starting " + threadCount + " concurrent threads...");

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                try {
                    String key = "concurrent:test:" + threadId;
                    String value = "Data from thread " + threadId;

                    // Каждый поток выполняет полный цикл операций
                    client.put(key.getBytes(), value.getBytes());
                    client.get(key.getBytes());
                    client.delete(key.getBytes());

                    System.out.println("  Thread " + threadId + " completed operations");

                } catch (Exception e) {
                    System.err.println("Thread " + threadId + " error: " + e.getMessage());
                }
            });
            threads[i].start();
        }

        // Ожидаем завершения всех потоков
        for (Thread thread : threads) {
            thread.join();
        }

        System.out.println("✓ All concurrent operations completed");
    }
}