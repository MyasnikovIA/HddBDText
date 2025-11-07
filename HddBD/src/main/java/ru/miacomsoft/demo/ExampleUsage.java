package ru.miacomsoft.demo;


import ru.miacomsoft.core.BinaryDataManager;
import ru.miacomsoft.core.SearchQuery;

public class ExampleUsage {
    public static void main(String[] args) {
        try (BinaryDataManager manager = new BinaryDataManager("/path/to/storage/data",  "/path/to/storage/index")) {

            // Запись данных
            byte[] key1 = "document1".getBytes();
            byte[] data1 = "Hello, World!".getBytes();
            manager.put(key1, data1);

            // Чтение данных
            byte[] result = manager.get(key1);
            System.out.println(new String(result)); // "Hello, World!"

            // Поиск по точному совпадению
            SearchQuery exactQuery = new SearchQuery.Builder()
                    .exactMatch(key1)
                    .build();
            var results = manager.find(exactQuery);
            System.out.println("Found: " + results.size() + " results");

            // Обновление данных
            byte[] newData = "Updated content".getBytes();
            manager.update(key1, newData);

            // Удаление данных
            manager.delete(key1);

            // Статистика
            System.out.println("Index size: " + manager.getIndexSize());
            System.out.println("Free space blocks: " + manager.getFreeSpaceManager().getFreeSpaceCount());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}