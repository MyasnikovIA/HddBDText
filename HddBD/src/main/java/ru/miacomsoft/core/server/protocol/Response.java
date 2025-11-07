package ru.miacomsoft.core.server.protocol;

import ru.miacomsoft.core.SqlResult;

import java.util.Arrays;
import java.util.List;

public class Response {
    private final boolean success;
    private final String message;
    private final byte[] data;
    private final List<byte[]> results;
    private final SystemStats stats;
    private final SqlResult sqlResult; // Добавляем SQL результат

    public Response(boolean success, String message) {
        this(success, message, null, null, null, null);
    }

    public Response(boolean success, String message, byte[] data) {
        this(success, message, data, null, null, null);
    }

    public Response(boolean success, String message, List<byte[]> results) {
        this(success, message, null, results, null, null);
    }

    public Response(boolean success, String message, SystemStats stats) {
        this(success, message, null, null, stats, null);
    }

    public Response(boolean success, String message, SqlResult sqlResult) {
        this(success, message, null, null, null, sqlResult);
    }

    public Response(boolean success, String message, byte[] data, List<byte[]> results, SystemStats stats, SqlResult sqlResult) {
        this.success = success;
        this.message = message;
        this.data = data;
        this.results = results;
        this.stats = stats;
        this.sqlResult = sqlResult;
    }

    // Getters
    public boolean isSuccess() { return success; }
    public String getMessage() { return message; }
    public byte[] getData() { return data; }
    public List<byte[]> getResults() { return results; }
    public SystemStats getStats() { return stats; }
    public SqlResult getSqlResult() { return sqlResult; }

    public static class SystemStats {
        private final int indexSize;
        private final long dataFileSize;
        private final int freeSpaceBlocks;
        private final long totalFreeSpace;
        private final long usedMemory;
        private final long maxMemory;
        private final double memoryUsageRatio;
        private final int cacheSize;

        public SystemStats(int indexSize, long dataFileSize, int freeSpaceBlocks,
                           long totalFreeSpace, long usedMemory, long maxMemory,
                           double memoryUsageRatio, int cacheSize) {
            this.indexSize = indexSize;
            this.dataFileSize = dataFileSize;
            this.freeSpaceBlocks = freeSpaceBlocks;
            this.totalFreeSpace = totalFreeSpace;
            this.usedMemory = usedMemory;
            this.maxMemory = maxMemory;
            this.memoryUsageRatio = memoryUsageRatio;
            this.cacheSize = cacheSize;
        }

        // Getters
        public int getIndexSize() { return indexSize; }
        public long getDataFileSize() { return dataFileSize; }
        public int getFreeSpaceBlocks() { return freeSpaceBlocks; }
        public long getTotalFreeSpace() { return totalFreeSpace; }
        public long getUsedMemory() { return usedMemory; }
        public long getMaxMemory() { return maxMemory; }
        public double getMemoryUsageRatio() { return memoryUsageRatio; }
        public int getCacheSize() { return cacheSize; }

        @Override
        public String toString() {
            return String.format(
                    "SystemStats{indexSize=%d, dataFileSize=%,d, freeBlocks=%d, freeSpace=%,d, " +
                            "memory=%,d/%,d (%.1f%%), cacheSize=%d}",
                    indexSize, dataFileSize, freeSpaceBlocks, totalFreeSpace,
                    usedMemory, maxMemory, memoryUsageRatio * 100, cacheSize
            );
        }
    }
}