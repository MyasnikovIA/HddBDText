package ru.miacomsoft.demo;

import ru.miacomsoft.core.*;

public class SqlDemo {
    public static void main(String[] args) {
        try (BinaryDataManager manager = BinaryDataManager.createWithDefaultMemory("sql_data.bin", "sql_index.idx")) {

            System.out.println("=== SQL Demo ===");

            // Пример 1: Создание таблицы
            System.out.println("\n--- Creating Table ---");
            SqlQuery createTable = SqlQuery.builder()
                    .createTable("users")
                    .column("id", "STRING")
                    .column("name", "STRING")
                    .column("age", "INT")
                    .column("email", "STRING")
                    .build();

            SqlResult result = manager.executeSql(createTable);
            System.out.println("Create table: " + result.getMessage());

            // Пример 2: Вставка данных
            System.out.println("\n--- Inserting Data ---");
            SqlQuery insert1 = SqlQuery.builder()
                    .insert("users")
                    .value("id", "user1")
                    .value("name", "John Doe")
                    .value("age", 30)
                    .value("email", "john@example.com")
                    .build();

            SqlQuery insert2 = SqlQuery.builder()
                    .insert("users")
                    .value("id", "user2")
                    .value("name", "Jane Smith")
                    .value("age", 25)
                    .value("email", "jane@example.com")
                    .build();

            result = manager.executeSql(insert1);
            System.out.println("Insert 1: " + result.getMessage());

            result = manager.executeSql(insert2);
            System.out.println("Insert 2: " + result.getMessage());

            // Пример 3: Выборка данных
            System.out.println("\n--- Selecting Data ---");
            SqlQuery selectAll = SqlQuery.builder()
                    .select("*")
                    .build(); // Покажет список таблиц

            result = manager.executeSql(selectAll);
            System.out.println("Tables: " + result.getRows());

            SqlQuery selectUsers = SqlQuery.builder()
                    .select("id", "name", "age")
                    .where("age", 30)
                    .build();

            // В реальной реализации здесь бы выполнялся поиск по условиям
            System.out.println("SELECT query constructed: " + selectUsers);

            // Пример 4: Обновление данных
            System.out.println("\n--- Updating Data ---");
            SqlQuery update = SqlQuery.builder()
                    .update("users")
                    .value("age", 31)
                    .where("id", "user1")
                    .build();

            result = manager.executeSql(update);
            System.out.println("Update: " + result.getMessage());

            // Пример 5: Удаление данных
            System.out.println("\n--- Deleting Data ---");
            SqlQuery delete = SqlQuery.builder()
                    .delete("users")
                    .where("id", "user2")
                    .build();

            result = manager.executeSql(delete);
            System.out.println("Delete: " + result.getMessage());

            // Пример 6: Удаление таблицы
            System.out.println("\n--- Dropping Table ---");
            SqlQuery dropTable = SqlQuery.builder()
                    .dropTable("users")
                    .build();

            result = manager.executeSql(dropTable);
            System.out.println("Drop table: " + result.getMessage());

            System.out.println("\n=== SQL Demo Completed ===");

        } catch (Exception e) {
            System.err.println("SQL demo error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}