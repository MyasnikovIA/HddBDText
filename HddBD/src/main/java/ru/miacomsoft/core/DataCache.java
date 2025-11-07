package ru.miacomsoft.core;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DataCache {
    private final Map<String, CacheEntry> cache;
    private final ScheduledExecutorService cleanupScheduler;
    private int cacheTTL = 3600; // 1 час по умолчанию

    private static class CacheEntry {
        byte[] data;
        long timestamp;
        HeaderRecord header;

        CacheEntry(byte[] data, HeaderRecord header) {
            this.data = data;
            this.header = header;
            this.timestamp = System.currentTimeMillis();
        }

        boolean isExpired(int ttlSeconds) {
            return (System.currentTimeMillis() - timestamp) > ttlSeconds * 1000L;
        }
    }

    public DataCache() {
        this.cache = new ConcurrentHashMap<>();
        this.cleanupScheduler = Executors.newSingleThreadScheduledExecutor();
        startCleanupTask();
    }

    public void put(byte[] key, byte[] data, HeaderRecord header) {
        String keyStr = Arrays.toString(key);
        cache.put(keyStr, new CacheEntry(data, header));
    }

    public byte[] get(byte[] key) {
        String keyStr = Arrays.toString(key);
        CacheEntry entry = cache.get(keyStr);

        if (entry != null && !entry.isExpired(cacheTTL)) {
            return entry.data;
        }

        if (entry != null && entry.isExpired(cacheTTL)) {
            cache.remove(keyStr);
        }

        return null;
    }

    public void remove(byte[] key) {
        String keyStr = Arrays.toString(key);
        cache.remove(keyStr);
    }

    public void setCacheTTL(int seconds) {
        this.cacheTTL = seconds;
    }

    private void startCleanupTask() {
        cleanupScheduler.scheduleAtFixedRate(() -> {
            cache.entrySet().removeIf(entry -> entry.getValue().isExpired(cacheTTL));
        }, 1, 1, TimeUnit.HOURS);
    }

    public void close() {
        cleanupScheduler.shutdown();
        try {
            if (!cleanupScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                cleanupScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            cleanupScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        cache.clear();
    }
    public int getSize() {
        return cache.size();
    }


}