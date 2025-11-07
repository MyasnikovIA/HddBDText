package ru.miacomsoft.core.server.protocol;

import ru.miacomsoft.core.SearchQuery;
import ru.miacomsoft.core.SqlQuery;

import java.util.Arrays;

public class Request {
    private final Command command;
    private final byte[] key;
    private final byte[] value;
    private final SearchQuery searchQuery;
    private final long expiryTime;
    private final float[] nameVector;
    private final float[] dataVector;
    private final SqlQuery sqlQuery; // Добавляем SQL запрос

    private Request(Builder builder) {
        this.command = builder.command;
        this.key = builder.key;
        this.value = builder.value;
        this.searchQuery = builder.searchQuery;
        this.expiryTime = builder.expiryTime;
        this.nameVector = builder.nameVector;
        this.dataVector = builder.dataVector;
        this.sqlQuery = builder.sqlQuery;
    }

    public static class Builder {
        private Command command;
        private byte[] key;
        private byte[] value;
        private SearchQuery searchQuery;
        private long expiryTime = -1;
        private float[] nameVector;
        private float[] dataVector;
        private SqlQuery sqlQuery;

        public Builder command(Command command) {
            this.command = command;
            return this;
        }

        public Builder key(byte[] key) {
            this.key = key;
            return this;
        }

        public Builder value(byte[] value) {
            this.value = value;
            return this;
        }

        public Builder searchQuery(SearchQuery query) {
            this.searchQuery = query;
            return this;
        }

        public Builder expiryTime(long expiryTime) {
            this.expiryTime = expiryTime;
            return this;
        }

        public Builder nameVector(float[] vector) {
            this.nameVector = vector;
            return this;
        }

        public Builder dataVector(float[] vector) {
            this.dataVector = vector;
            return this;
        }

        public Builder sqlQuery(SqlQuery sqlQuery) {
            this.sqlQuery = sqlQuery;
            return this;
        }

        public Request build() {
            return new Request(this);
        }
    }

    // Getters
    public Command getCommand() { return command; }
    public byte[] getKey() { return key; }
    public byte[] getValue() { return value; }
    public SearchQuery getSearchQuery() { return searchQuery; }
    public long getExpiryTime() { return expiryTime; }
    public float[] getNameVector() { return nameVector; }
    public float[] getDataVector() { return dataVector; }
    public SqlQuery getSqlQuery() { return sqlQuery; }

    @Override
    public String toString() {
        return "Request{" +
                "command=" + command +
                ", key=" + Arrays.toString(key) +
                ", valueSize=" + (value != null ? value.length : 0) +
                ", expiryTime=" + expiryTime +
                ", sqlQuery=" + (sqlQuery != null ? sqlQuery.toString() : "null") +
                '}';
    }
}