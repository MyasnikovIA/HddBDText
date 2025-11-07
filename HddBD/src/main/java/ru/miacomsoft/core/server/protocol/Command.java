package ru.miacomsoft.core.server.protocol;

public enum Command {
    PUT(1),
    GET(2),
    UPDATE(3),
    DELETE(4),
    FIND(5),
    PING(6),
    STATS(7),
    SQL_EXECUTE(8),      // Выполнение SQL запроса
    SQL_QUERY(9),        // Выполнение SQL запроса с возвратом результатов
    SQL_CREATE_TABLE(10), // Создание таблицы
    SQL_DROP_TABLE(11),   // Удаление таблицы
    SQL_INSERT(12),       // Вставка данных
    SQL_SELECT(13),       // Выборка данных
    SQL_UPDATE(14),       // Обновление данных
    SQL_DELETE(15),       // Удаление данных
    SQL_CREATE_INDEX(16), // Создание индекса
    SQL_ADD_RELATION(17); // Добавление связи

    private final int code;

    Command(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static Command fromCode(int code) {
        for (Command cmd : values()) {
            if (cmd.code == code) {
                return cmd;
            }
        }
        throw new IllegalArgumentException("Unknown command code: " + code);
    }
}