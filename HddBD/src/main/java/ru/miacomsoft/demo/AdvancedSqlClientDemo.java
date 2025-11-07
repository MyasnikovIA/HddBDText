package ru.miacomsoft.demo;

import ru.miacomsoft.core.client.DataClient;
import ru.miacomsoft.core.SqlQuery;
import ru.miacomsoft.core.SqlResult;
import ru.miacomsoft.core.server.protocol.Response;

import java.util.HashMap;
import java.util.Map;

public class AdvancedSqlClientDemo {
    public static void main(String[] args) {
        // Исправляем порт - должен быть в диапазоне 0-65535
        try (DataClient client = new DataClient("localhost", 8080)) {

            System.out.println("=== Advanced SQL Client Demo ===");
            client.connect();
            System.out.println("✓ Connected to server");

            // Проверяем соединение
            if (client.ping()) {
                System.out.println("✓ Server is responsive");
            }

            // Получаем статистику системы
            Response.SystemStats stats = client.getStats();
            if (stats != null) {
                System.out.println("✓ System Statistics:");
                System.out.println("  - Memory usage: " + String.format("%.1f", stats.getMemoryUsageRatio() * 100) + "%");
                System.out.println("  - Cache size: " + stats.getCacheSize() + " items");
            }

            // Создаем сложную схему базы данных
            System.out.println("\n--- Creating Database Schema ---");

            // Таблица сотрудников
            Map<String, String> employeesSchema = new HashMap<>();
            employeesSchema.put("emp_id", "STRING");
            employeesSchema.put("first_name", "STRING");
            employeesSchema.put("last_name", "STRING");
            employeesSchema.put("department", "STRING");
            employeesSchema.put("salary", "DOUBLE");
            employeesSchema.put("hire_date", "LONG");

            SqlResult result = client.createTable("employees", employeesSchema);
            System.out.println("Create employees table: " + result.getMessage());

            // Таблица отделов
            Map<String, String> departmentsSchema = new HashMap<>();
            departmentsSchema.put("dept_id", "STRING");
            departmentsSchema.put("dept_name", "STRING");
            departmentsSchema.put("manager_id", "STRING");
            departmentsSchema.put("budget", "DOUBLE");

            result = client.createTable("departments", departmentsSchema);
            System.out.println("Create departments table: " + result.getMessage());

            // Таблица проектов
            Map<String, String> projectsSchema = new HashMap<>();
            projectsSchema.put("project_id", "STRING");
            projectsSchema.put("project_name", "STRING");
            projectsSchema.put("dept_id", "STRING");
            projectsSchema.put("start_date", "LONG");
            projectsSchema.put("end_date", "LONG");
            projectsSchema.put("budget", "DOUBLE");

            result = client.createTable("projects", projectsSchema);
            System.out.println("Create projects table: " + result.getMessage());

            // Заполняем данными
            System.out.println("\n--- Populating Data ---");

            // Добавляем отделы
            Map<String, Object> dept1 = Map.of(
                    "dept_id", "D001",
                    "dept_name", "Engineering",
                    "manager_id", "E001",
                    "budget", 1000000.0
            );
            client.insert("departments", dept1);

            Map<String, Object> dept2 = Map.of(
                    "dept_id", "D002",
                    "dept_name", "Marketing",
                    "manager_id", "E002",
                    "budget", 500000.0
            );
            client.insert("departments", dept2);

            // Добавляем сотрудников
            Map<String, Object> emp1 = Map.of(
                    "emp_id", "E001",
                    "first_name", "Alice",
                    "last_name", "Johnson",
                    "department", "D001",
                    "salary", 80000.0,
                    "hire_date", System.currentTimeMillis() - (365L * 24 * 60 * 60 * 1000) // 1 year ago
            );
            client.insert("employees", emp1);

            Map<String, Object> emp2 = Map.of(
                    "emp_id", "E002",
                    "first_name", "Bob",
                    "last_name", "Smith",
                    "department", "D002",
                    "salary", 70000.0,
                    "hire_date", System.currentTimeMillis() - (180L * 24 * 60 * 60 * 1000) // 6 months ago
            );
            client.insert("employees", emp2);

            Map<String, Object> emp3 = Map.of(
                    "emp_id", "E003",
                    "first_name", "Carol",
                    "last_name", "Williams",
                    "department", "D001",
                    "salary", 60000.0,
                    "hire_date", System.currentTimeMillis() - (90L * 24 * 60 * 60 * 1000) // 3 months ago
            );
            client.insert("employees", emp3);

            // Добавляем проекты
            Map<String, Object> project1 = Map.of(
                    "project_id", "P001",
                    "project_name", "Website Redesign",
                    "dept_id", "D002",
                    "start_date", System.currentTimeMillis(),
                    "end_date", System.currentTimeMillis() + (90L * 24 * 60 * 60 * 1000),
                    "budget", 50000.0
            );
            client.insert("projects", project1);

            Map<String, Object> project2 = Map.of(
                    "project_id", "P002",
                    "project_name", "Mobile App Development",
                    "dept_id", "D001",
                    "start_date", System.currentTimeMillis(),
                    "end_date", System.currentTimeMillis() + (180L * 24 * 60 * 60 * 1000),
                    "budget", 150000.0
            );
            client.insert("projects", project2);

            // Создаем индексы для ускорения поиска
            System.out.println("\n--- Creating Indexes ---");
            client.createIndex("employees", "department");
            client.createIndex("employees", "salary");
            client.createIndex("projects", "dept_id");

            System.out.println("✓ Indexes created for better performance");

            // Создаем связи между таблицами
            System.out.println("\n--- Creating Relations ---");
            client.addRelation("departments", "employees", "dept_employees");
            client.addRelation("departments", "projects", "dept_projects");

            System.out.println("✓ Relations established between tables");

            // Демонстрация сложных операций
            System.out.println("\n--- Complex Operations ---");

            // Обновление зарплаты
            Map<String, Object> salaryUpdate = Map.of("salary", 85000.0);
            Map<String, Object> salaryWhere = Map.of("emp_id", "E001");
            result = client.update("employees", salaryUpdate, salaryWhere);
            System.out.println("Salary update: " + result.getMessage());

            // Удаление тестового сотрудника
            Map<String, Object> deleteWhere = Map.of("emp_id", "E003");
            result = client.delete("employees", deleteWhere);
            System.out.println("Employee deletion: " + result.getMessage());

            // Показываем список таблиц
            result = client.select(null);
            if (result.hasRows()) {
                System.out.println("\n✓ Database schema created successfully:");
                for (Map<String, Object> row : result.getRows()) {
                    System.out.println("  - " + row.get("table_name"));
                }
            }

            // Очистка
            System.out.println("\n--- Cleanup ---");
            client.dropTable("projects");
            client.dropTable("employees");
            client.dropTable("departments");

            System.out.println("✓ Database cleaned up");

            System.out.println("\n=== Advanced SQL Demo Completed ===");

        } catch (Exception e) {
            System.err.println("Advanced SQL client error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}