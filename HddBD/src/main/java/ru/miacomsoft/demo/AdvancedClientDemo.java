package ru.miacomsoft.demo;

import ru.miacomsoft.core.client.DataClient;
import ru.miacomsoft.core.SearchQuery;
import ru.miacomsoft.core.server.protocol.Response;

import java.util.List;

public class AdvancedClientDemo {
    public static void main(String[] args) {
        try (DataClient client = new DataClient("localhost", 8080)) {

            System.out.println("=== Advanced Client Demo ===");
            client.connect();
            System.out.println("✓ Connected to server");

            // Пример 1: Хранение различных типов данных
            System.out.println("\n--- Storing Different Data Types ---");

            // Текстовые данные
            storeTextData(client, "document:1", "This is a text document content");

            // JSON данные
            storeJsonData(client, "config:app", "{\"version\": \"1.0.0\", \"features\": [\"auth\", \"storage\"]}");

            // Бинарные данные (симуляция изображения)
            storeBinaryData(client, "image:thumbnail", new byte[]{0x01, 0x02, 0x03, 0x04, 0x05});

            // Данные с истечением срока (10 секунд)
            storeDataWithExpiry(client, "temp:session", "Temporary session data", 10);

            // Пример 2: Поиск данных
            System.out.println("\n--- Search Operations ---");

            // Поиск по точному совпадению
            SearchQuery exactQuery = new SearchQuery.Builder().exactMatch("document:1".getBytes()).build();

            List<byte[]> exactResults = client.find(exactQuery);
            if (exactResults != null) {
                System.out.println("✓ Exact search found: " + exactResults.size() + " results");
                for (byte[] data : exactResults) {
                    System.out.println("  - " + new String(data));
                }
            }

            // Поиск по маске (все документы)
            SearchQuery maskQuery = new SearchQuery.Builder()
                    .maskSearch("document:?".getBytes()) // '?' - любой символ
                    .build();

            List<byte[]> maskResults = client.find(maskQuery);
            if (maskResults != null) {
                System.out.println("✓ Mask search found: " + maskResults.size() + " results");
            }

            // Пример 3: Пакетные операции
            System.out.println("\n--- Batch Operations ---");

            // Сохраняем несколько записей
            for (int i = 1; i <= 5; i++) {
                String key = "batch:item:" + i;
                String value = "Batch data item " + i;
                if (client.put(key.getBytes(), value.getBytes())) {
                    System.out.println("✓ Stored: " + key);
                }
            }

            // Читаем несколько записей
            for (int i = 1; i <= 3; i++) {
                String key = "batch:item:" + i;
                byte[] data = client.get(key.getBytes());
                if (data != null) {
                    System.out.println("✓ Retrieved: " + key + " -> " + new String(data));
                }
            }

            // Пример 4: Мониторинг и статистика
            System.out.println("\n--- Monitoring ---");

            Response.SystemStats stats = client.getStats();
            if (stats != null) {
                System.out.println("✓ System Statistics:");
                System.out.println("  - Index size: " + stats.getIndexSize() + " records");
                System.out.println("  - Data file size: " + stats.getDataFileSize() + " bytes");
                System.out.println("  - Free space: " + stats.getTotalFreeSpace() + " bytes in " +
                        stats.getFreeSpaceBlocks() + " blocks");
                System.out.println("  - Memory usage: " + stats.getUsedMemory() + "/" +
                        stats.getMaxMemory() + " bytes (" +
                        String.format("%.1f", stats.getMemoryUsageRatio() * 100) + "%)");
                System.out.println("  - Cache size: " + stats.getCacheSize() + " items");
            }

            // Пример 5: Очистка тестовых данных
            System.out.println("\n--- Cleanup ---");

            String[] keysToDelete = {
                    "document:1", "config:app", "image:thumbnail", "temp:session"
            };

            for (String key : keysToDelete) {
                if (client.delete(key.getBytes())) {
                    System.out.println("✓ Deleted: " + key);
                }
            }

            // Удаляем пакетные данные
            for (int i = 1; i <= 5; i++) {
                String key = "batch:item:" + i;
                if (client.delete(key.getBytes())) {
                    System.out.println("✓ Deleted: " + key);
                }
            }

            System.out.println("\n=== Advanced Demo Completed ===");

        } catch (Exception e) {
            System.err.println("Advanced client error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void storeTextData(DataClient client, String key, String text) throws Exception {
        if (client.put(key.getBytes(), text.getBytes())) {
            System.out.println("✓ Stored text data: " + key);
        }
    }

    private static void storeJsonData(DataClient client, String key, String json) throws Exception {
        if (client.put(key.getBytes(), json.getBytes())) {
            System.out.println("✓ Stored JSON data: " + key);
        }
    }

    private static void storeBinaryData(DataClient client, String key, byte[] data) throws Exception {
        if (client.put(key.getBytes(), data)) {
            System.out.println("✓ Stored binary data: " + key + " (" + data.length + " bytes)");
        }
    }

    private static void storeDataWithExpiry(DataClient client, String key, String data, int seconds) throws Exception {
        long expiryTime = System.currentTimeMillis() / 1000 + seconds;
        if (client.put(key.getBytes(), data.getBytes(), expiryTime, null, null)) {
            System.out.println("✓ Stored data with expiry: " + key + " (expires in " + seconds + "s)");
        }
    }
}