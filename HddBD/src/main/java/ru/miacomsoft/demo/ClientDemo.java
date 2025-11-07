package ru.miacomsoft.demo;

import ru.miacomsoft.core.client.DataClient;
import ru.miacomsoft.core.SearchQuery;
import ru.miacomsoft.core.server.protocol.Response;

public class ClientDemo {
    public static void main(String[] args) {
        try (DataClient client = new DataClient("localhost", 8080)) {
            client.connect();

            // Тестируем соединение
            if (client.ping()) {
                System.out.println("Server is responsive");
            }

            // Записываем данные
            byte[] key1 = "document1".getBytes();
            byte[] data1 = "Hello, World!".getBytes();
            if (client.put(key1, data1)) {
                System.out.println("Data stored successfully");
            }

            // Читаем данные
            byte[] result = client.get(key1);
            if (result != null) {
                System.out.println("Retrieved: " + new String(result));
            }

            // Поиск данных
            SearchQuery query = new SearchQuery.Builder()
                    .exactMatch(key1)
                    .build();
            var results = client.find(query);
            if (results != null) {
                System.out.println("Search found: " + results.size() + " results");
            }

            // Получаем статистику
            Response.SystemStats stats = client.getStats();
            if (stats != null) {
                System.out.println("Server stats: " + stats);
            }

            // Обновляем данные
            byte[] newData = "Updated content".getBytes();
            if (client.update(key1, newData)) {
                System.out.println("Data updated successfully");
            }

            // Удаляем данные
            if (client.delete(key1)) {
                System.out.println("Data deleted successfully");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}