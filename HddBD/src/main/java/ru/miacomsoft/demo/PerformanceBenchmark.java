package ru.miacomsoft.demo;

import ru.miacomsoft.core.client.DataClient;

public class PerformanceBenchmark {
    private static final int OPERATION_COUNT = 1000;
    private static final int DATA_SIZE = 1024; // 1KB per operation

    public static void main(String[] args) {
        System.out.println("=== Performance Benchmark ===");
        System.out.println("Operations: " + OPERATION_COUNT);
        System.out.println("Data size per operation: " + DATA_SIZE + " bytes");
        System.out.println("Total data: " + (OPERATION_COUNT * DATA_SIZE / 1024) + " KB");

        try (DataClient client = new DataClient("localhost", 8080)) {
            client.connect();
            System.out.println("✓ Connected to server");

            // Бенчмарк записи
            long writeTime = benchmarkWriteOperations(client);
            System.out.println("Write performance: " + OPERATION_COUNT + " operations in " +
                    writeTime + "ms (" + (OPERATION_COUNT * 1000.0 / writeTime) + " ops/sec)");

            // Бенчмарк чтения
            long readTime = benchmarkReadOperations(client);
            System.out.println("Read performance: " + OPERATION_COUNT + " operations in " +
                    readTime + "ms (" + (OPERATION_COUNT * 1000.0 / readTime) + " ops/sec)");

            // Очистка тестовых данных
            cleanupTestData(client);

        } catch (Exception e) {
            System.err.println("Benchmark error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static long benchmarkWriteOperations(DataClient client) throws Exception {
        System.out.println("\n--- Benchmarking Write Operations ---");

        byte[] testData = new byte[DATA_SIZE];
        for (int i = 0; i < DATA_SIZE; i++) {
            testData[i] = (byte) (i % 256);
        }

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            String key = "benchmark:write:" + i;
            if (!client.put(key.getBytes(), testData)) {
                throw new RuntimeException("Write operation failed for key: " + key);
            }

            // Прогресс каждые 100 операций
            if ((i + 1) % 100 == 0) {
                System.out.println("  Completed " + (i + 1) + " write operations");
            }
        }

        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }

    private static long benchmarkReadOperations(DataClient client) throws Exception {
        System.out.println("\n--- Benchmarking Read Operations ---");

        long startTime = System.currentTimeMillis();
        int successCount = 0;

        for (int i = 0; i < OPERATION_COUNT; i++) {
            String key = "benchmark:write:" + i;
            byte[] data = client.get(key.getBytes());

            if (data != null && data.length == DATA_SIZE) {
                successCount++;
            }

            // Прогресс каждые 100 операций
            if ((i + 1) % 100 == 0) {
                System.out.println("  Completed " + (i + 1) + " read operations");
            }
        }

        long endTime = System.currentTimeMillis();

        if (successCount != OPERATION_COUNT) {
            System.err.println("Warning: Only " + successCount + "/" + OPERATION_COUNT + " read operations succeeded");
        }

        return endTime - startTime;
    }

    private static void cleanupTestData(DataClient client) throws Exception {
        System.out.println("\n--- Cleaning up test data ---");

        for (int i = 0; i < OPERATION_COUNT; i++) {
            String key = "benchmark:write:" + i;
            client.delete(key.getBytes());
        }

        System.out.println("✓ Cleanup completed");
    }
}