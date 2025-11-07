package ru.miacomsoft.demo;

import ru.miacomsoft.core.client.DataClient;
import ru.miacomsoft.core.SqlQuery;
import ru.miacomsoft.core.SqlResult;

import java.util.HashMap;
import java.util.Map;

public class SqlClientDemo {
    public static void main(String[] args) {
        try (DataClient client = new DataClient("localhost", 8080)) {

            System.out.println("=== SQL Client Demo ===");
            client.connect();
            System.out.println("✓ Connected to server");

            // Пример 1: Создание таблицы пользователей
            System.out.println("\n--- Creating Users Table ---");
            Map<String, String> userSchema = new HashMap<>();
            userSchema.put("id", "STRING");
            userSchema.put("name", "STRING");
            userSchema.put("age", "INT");
            userSchema.put("email", "STRING");
            userSchema.put("created_at", "LONG");

            SqlResult result = client.createTable("users", userSchema);
            System.out.println("Create table: " + result.getMessage());

            // Пример 2: Вставка данных в таблицу
            System.out.println("\n--- Inserting User Data ---");

            Map<String, Object> user1 = new HashMap<>();
            user1.put("id", "user_001");
            user1.put("name", "John Doe");
            user1.put("age", 30);
            user1.put("email", "john.doe@example.com");
            user1.put("created_at", System.currentTimeMillis());

            result = client.insert("users", user1);
            System.out.println("Insert user 1: " + result.getMessage());

            Map<String, Object> user2 = new HashMap<>();
            user2.put("id", "user_002");
            user2.put("name", "Jane Smith");
            user2.put("age", 25);
            user2.put("email", "jane.smith@example.com");
            user2.put("created_at", System.currentTimeMillis());

            result = client.insert("users", user2);
            System.out.println("Insert user 2: " + result.getMessage());

            Map<String, Object> user3 = new HashMap<>();
            user3.put("id", "user_003");
            user3.put("name", "Bob Johnson");
            user3.put("age", 35);
            user3.put("email", "bob.johnson@example.com");
            user3.put("created_at", System.currentTimeMillis());

            result = client.insert("users", user3);
            System.out.println("Insert user 3: " + result.getMessage());

            // Пример 3: Создание таблицы заказов
            System.out.println("\n--- Creating Orders Table ---");
            Map<String, String> orderSchema = new HashMap<>();
            orderSchema.put("order_id", "STRING");
            orderSchema.put("user_id", "STRING");
            orderSchema.put("product", "STRING");
            orderSchema.put("amount", "DOUBLE");
            orderSchema.put("order_date", "LONG");

            result = client.createTable("orders", orderSchema);
            System.out.println("Create table: " + result.getMessage());

            // Пример 4: Вставка заказов
            System.out.println("\n--- Inserting Order Data ---");

            Map<String, Object> order1 = new HashMap<>();
            order1.put("order_id", "order_001");
            order1.put("user_id", "user_001");
            order1.put("product", "Laptop");
            order1.put("amount", 999.99);
            order1.put("order_date", System.currentTimeMillis());

            result = client.insert("orders", order1);
            System.out.println("Insert order 1: " + result.getMessage());

            Map<String, Object> order2 = new HashMap<>();
            order2.put("order_id", "order_002");
            order2.put("user_id", "user_002");
            order2.put("product", "Smartphone");
            order2.put("amount", 499.99);
            order2.put("order_date", System.currentTimeMillis());

            result = client.insert("orders", order2);
            System.out.println("Insert order 2: " + result.getMessage());

            // Пример 5: Создание индекса
            System.out.println("\n--- Creating Indexes ---");
            result = client.createIndex("users", "email");
            System.out.println("Create email index: " + result.getMessage());

            result = client.createIndex("orders", "user_id");
            System.out.println("Create user_id index: " + result.getMessage());

            // Пример 6: Создание связи между таблицами
            System.out.println("\n--- Creating Relations ---");
            result = client.addRelation("users", "orders", "user_orders");
            System.out.println("Create relation: " + result.getMessage());

            // Пример 7: Обновление данных
            System.out.println("\n--- Updating Data ---");
            Map<String, Object> updateValues = new HashMap<>();
            updateValues.put("age", 31);

            Map<String, Object> whereConditions = new HashMap<>();
            whereConditions.put("id", "user_001");

            result = client.update("users", updateValues, whereConditions);
            System.out.println("Update user age: " + result.getMessage());

            // Пример 8: Выборка данных (список таблиц)
            System.out.println("\n--- Listing Tables ---");
            result = client.select(null); // SELECT без таблицы показывает список таблиц
            if (result.isSuccess() && result.hasRows()) {
                System.out.println("Available tables:");
                for (Map<String, Object> row : result.getRows()) {
                    System.out.println("  - " + row.get("table_name"));
                }
            }

            // Пример 9: Удаление данных
            System.out.println("\n--- Deleting Data ---");
            Map<String, Object> deleteConditions = new HashMap<>();
            deleteConditions.put("id", "user_003");

            result = client.delete("users", deleteConditions);
            System.out.println("Delete user: " + result.getMessage());

            // Пример 10: Удаление таблицы
            System.out.println("\n--- Cleaning Up ---");
            result = client.dropTable("orders");
            System.out.println("Drop orders table: " + result.getMessage());

            result = client.dropTable("users");
            System.out.println("Drop users table: " + result.getMessage());

            System.out.println("\n=== SQL Client Demo Completed ===");

        } catch (Exception e) {
            System.err.println("SQL client error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}