package ru.miacomsoft.core;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MemoryManager implements AutoCloseable {
    private final long maxMemoryBytes;
    private final MemoryMXBean memoryBean;
    private final ScheduledExecutorService monitorScheduler;
    private long usedMemory;
    private boolean memoryLimitEnabled;

    // Константы по умолчанию
    private static final long DEFAULT_MEMORY_LIMIT = 100 * 1024 * 1024; // 100 MB
    private static final long MAX_MEMORY_LIMIT = 100L * 1024 * 1024 * 1024; // 100 GB

    public MemoryManager() {
        this(DEFAULT_MEMORY_LIMIT);
    }

    public MemoryManager(long maxMemoryBytes) {
        if (maxMemoryBytes <= 0 || maxMemoryBytes > MAX_MEMORY_LIMIT) {
            throw new IllegalArgumentException("Memory limit must be between 1 byte and 100 GB");
        }

        this.maxMemoryBytes = maxMemoryBytes;
        this.memoryBean = ManagementFactory.getMemoryMXBean();
        this.monitorScheduler = Executors.newSingleThreadScheduledExecutor();
        this.usedMemory = 0;
        this.memoryLimitEnabled = true;

        startMemoryMonitoring();
    }

    public synchronized boolean allocateMemory(long bytes) {
        if (!memoryLimitEnabled) {
            usedMemory += bytes;
            return true;
        }

        if (usedMemory + bytes <= maxMemoryBytes) {
            usedMemory += bytes;
            return true;
        }
        return false;
    }

    public synchronized void releaseMemory(long bytes) {
        usedMemory = Math.max(0, usedMemory - bytes);
    }

    public synchronized long getUsedMemory() {
        return usedMemory;
    }

    public long getMaxMemory() {
        return maxMemoryBytes;
    }

    public double getMemoryUsageRatio() {
        return (double) usedMemory / maxMemoryBytes;
    }

    public boolean isMemoryLimitExceeded() {
        return usedMemory > maxMemoryBytes;
    }

    public void setMemoryLimitEnabled(boolean enabled) {
        this.memoryLimitEnabled = enabled;
    }

    public void forceGarbageCollection() {
        System.gc();
    }

    public MemoryUsage getHeapMemoryUsage() {
        return memoryBean.getHeapMemoryUsage();
    }

    public MemoryUsage getNonHeapMemoryUsage() {
        return memoryBean.getNonHeapMemoryUsage();
    }

    private void startMemoryMonitoring() {
        monitorScheduler.scheduleAtFixedRate(() -> {
            MemoryUsage heapUsage = getHeapMemoryUsage();
            double usageRatio = getMemoryUsageRatio();

            if (usageRatio > 0.9) {
                System.gc();
            }

            if (usageRatio > 0.95) {
                System.err.println("WARNING: Memory usage critical: " +
                        (usageRatio * 100) + "% (" + usedMemory + "/" + maxMemoryBytes + " bytes)");
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    public void close() {
        monitorScheduler.shutdown();
        try {
            if (!monitorScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                monitorScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            monitorScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    // Заменяем устаревший finalize на AutoCloseable
    @Override
    public void finalize() {
        close();
    }
}