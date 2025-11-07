package ru.miacomsoft.demo;

import ru.miacomsoft.core.client.DataClient;
import ru.miacomsoft.core.SearchQuery;
import ru.miacomsoft.core.server.protocol.Response;

public class BasicClientDemo {
    public static void main(String[] args) {
        // Создаем клиент
        try (DataClient client = new DataClient("localhost", 8080)) {

            System.out.println("=== Basic Client Demo ===");

            // Подключаемся к серверу
            client.connect();
            System.out.println("✓ Connected to server");

            // Проверяем соединение
            if (client.ping()) {
                System.out.println("✓ Server is responsive");
            }

            // Пример 1: Базовые операции CRUD
            System.out.println("\n--- CRUD Operations ---");

            byte[] key1 = "user:1001".getBytes();
            byte[] data1 = "{\"name\": \"John Doe\", \"age\": 30, \"email\": \"john@example.com\"}".getBytes();

            // Запись данных
            if (client.put(key1, data1)) {
                System.out.println("✓ Data stored with key: " + new String(key1));
            }

            // Чтение данных
            byte[] result = client.get(key1);
            if (result != null) {
                System.out.println("✓ Retrieved data: " + new String(result));
            }

            // Обновление данных
            byte[] updatedData = "{\"name\": \"John Doe\", \"age\": 31, \"email\": \"john.doe@example.com\"}".getBytes();
            if (client.update(key1, updatedData)) {
                System.out.println("✓ Data updated successfully");
            }

            // Проверяем обновление
            result = client.get(key1);
            if (result != null) {
                System.out.println("✓ Updated data: " + new String(result));
            }

            // Удаление данных
            if (client.delete(key1)) {
                System.out.println("✓ Data deleted successfully");
            }

            // Проверяем удаление
            result = client.get(key1);
            if (result == null) {
                System.out.println("✓ Data confirmed deleted");
            }

            // Получаем статистику
            Response.SystemStats stats = client.getStats();
            if (stats != null) {
                System.out.println("\n--- System Statistics ---");
                System.out.println(stats);
            }

            System.out.println("\n=== Demo completed successfully ===");

        } catch (Exception e) {
            System.err.println("Client error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}