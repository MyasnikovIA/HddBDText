package ru.miacomsoft.core;

import ru.miacomsoft.core.exceptions.DataManagerException;
import ru.miacomsoft.core.exceptions.KeyNotFoundException;

import java.io.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BinaryDataManager implements AutoCloseable {
    private final RandomAccessFile dataFile;
    private final HeaderIndex headerIndex;
    private final DataCache cache;
    private final FreeSpaceManager freeSpaceManager;
    private final MemoryManager memoryManager;
    private final ReadWriteLock dataFileLock;
    private final ScheduledExecutorService backgroundScheduler;
    private final String dataFileName;
    private final String indexFileName;
    private final SqlProcessor sqlProcessor;

    // Константы разделителей
    private static final byte[] HEADER_DATA_DELIMITER = "%%HEADER_DATA%%".getBytes();
    private static final byte[] DATA_VECTOR_DELIMITER = "%%DATA_VECTOR%%".getBytes();
    private static final byte[] RECORD_END_DELIMITER = "%%RECORD_END%%".getBytes();

    // Константы памяти
    private static final long DEFAULT_MEMORY_LIMIT = 100 * 1024 * 1024; // 100 MB
    private static final long MAX_MEMORY_LIMIT = 100L * 1024 * 1024 * 1024; // 100 GB

    public BinaryDataManager(String dataFileName, String indexFileName) throws IOException {
        this(dataFileName, indexFileName, DEFAULT_MEMORY_LIMIT);
    }

    public BinaryDataManager(String dataFileName, String indexFileName, long maxMemoryBytes) throws IOException {
        this(dataFileName, indexFileName, new MemoryManager(maxMemoryBytes));
    }

    public BinaryDataManager(String dataFileName, String indexFileName, MemoryManager memoryManager) throws IOException {
        this.dataFileName = dataFileName;
        this.indexFileName = indexFileName;
        this.memoryManager = memoryManager;

        this.sqlProcessor = new SqlProcessor(this);

        // Создаем директорию если не существует
        File baseDataDir = new File(dataFileName);
        File baseIndexDir = new File(indexFileName);

        if (baseDataDir.getParentFile() != null && !baseDataDir.getParentFile().exists()) {
            baseDataDir.getParentFile().mkdirs();
        }

        if (baseIndexDir.getParentFile() != null && !baseIndexDir.getParentFile().exists()) {
            baseIndexDir.getParentFile().mkdirs();
        }

        // Инициализация файла данных
        this.dataFile = new RandomAccessFile(dataFileName, "rw");

        // Инициализация индекса заголовков
        this.headerIndex = new HeaderIndex(indexFileName);

        // Инициализация остальных компонентов
        this.cache = new DataCache();
        this.freeSpaceManager = new FreeSpaceManager();
        this.dataFileLock = new ReentrantReadWriteLock();
        this.backgroundScheduler = Executors.newScheduledThreadPool(2);

        startBackgroundTasks();
        initializeFreeSpaceManager();

        System.out.println("BinaryDataManager initialized:");
        System.out.println("  Data file: " + dataFileName);
        System.out.println("  Index file: " + indexFileName);
        System.out.println("  Memory limit: " + (memoryManager.getMaxMemory() / (1024 * 1024)) + " MB");
    }

    // Основные методы API

    public byte[] get(byte[] key) {
        // 1. Проверка кэша
        byte[] cachedData = cache.get(key);
        if (cachedData != null) {
            return cachedData;
        }

        dataFileLock.readLock().lock();
        try {
            // 2. Поиск в Header Index
            HeaderRecord header = headerIndex.get(key);
            if (header == null || !header.isActive || isExpired(header.expiryTime)) {
                return null;
            }

            // 3. Чтение данных из файла
            byte[] data = readDataBlock(header);
            if (data != null) {
                // Выделяем память для кэша
                long memoryNeeded = estimateMemoryUsage(data, header);
                if (memoryManager.allocateMemory(memoryNeeded)) {
                    cache.put(key, data, header);
                } else {
                    System.err.println("Warning: Cannot cache data due to memory limit");
                }
            }

            return data;
        } catch (IOException e) {
            throw new DataManagerException("Error reading data for key: " + Arrays.toString(key), e);
        } finally {
            dataFileLock.readLock().unlock();
        }
    }

    public void put(byte[] key, byte[] value) {
        put(key, value, -1, null, null);
    }

    public void put(byte[] key, byte[] value, long expiryTime, float[] nameVector, float[] dataVector) {
        if (key == null || key.length == 0) {
            throw new DataManagerException("Key cannot be null or empty");
        }
        if (value == null) {
            throw new DataManagerException("Value cannot be null");
        }

        // Оценка необходимой памяти
        long memoryNeeded = estimateMemoryUsage(key, value, nameVector, dataVector);
        if (!memoryManager.allocateMemory(memoryNeeded)) {
            throw new DataManagerException("Memory limit exceeded. Required: " + memoryNeeded +
                    ", Available: " + (memoryManager.getMaxMemory() - memoryManager.getUsedMemory()));
        }

        dataFileLock.writeLock().lock();
        try {
            // Проверка существования блока
            HeaderRecord existing = headerIndex.get(key);
            if (existing != null && existing.isActive && !isExpired(existing.expiryTime)) {
                // Помечаем старый блок на удаление
                markForDeletion(existing);
            }

            // Поиск свободного места или запись в конец
            int totalBlockSize = calculateTotalBlockSize(key, value, dataVector);
            long dataAddress = freeSpaceManager.findFreeSpace(totalBlockSize);
            if (dataAddress == -1) {
                dataAddress = dataFile.length();
            }

            // Запись блока данных
            writeDataBlock(key, value, expiryTime, dataVector, dataAddress);

            // Обновление Header Index
            HeaderRecord newHeader = new HeaderRecord(true, expiryTime, key,
                    dataAddress, value.length, nameVector);
            headerIndex.put(key, newHeader);

            // Обновление кэша
            cache.put(key, value, newHeader);

        } catch (IOException e) {
            // Освобождаем память в случае ошибки
            memoryManager.releaseMemory(memoryNeeded);
            throw new DataManagerException("Error writing data for key: " + Arrays.toString(key), e);
        } finally {
            dataFileLock.writeLock().unlock();
        }
    }

    public void update(byte[] key, byte[] newValue) {
        if (key == null || key.length == 0) {
            throw new DataManagerException("Key cannot be null or empty");
        }
        if (newValue == null) {
            throw new DataManagerException("New value cannot be null");
        }

        dataFileLock.writeLock().lock();
        try {
            HeaderRecord existing = headerIndex.get(key);
            if (existing == null || !existing.isActive) {
                throw new KeyNotFoundException(key);
            }

            // Оценка необходимой памяти
            long memoryNeeded = estimateMemoryUsage(key, newValue, existing.nameVector, null);
            if (!memoryManager.allocateMemory(memoryNeeded)) {
                throw new DataManagerException("Memory limit exceeded for update operation");
            }

            try {
                if (newValue.length <= existing.dataSize) {
                    // Перезапись на том же месте
                    updateDataBlock(existing, newValue);

                    // Обновление Header Index
                    HeaderRecord updatedHeader = new HeaderRecord(true, existing.expiryTime,
                            existing.name, existing.dataAddress, newValue.length, existing.nameVector);
                    headerIndex.put(key, updatedHeader);

                    // Обновление кэша
                    cache.put(key, newValue, updatedHeader);
                } else {
                    // Новое размещение
                    put(key, newValue, existing.expiryTime, existing.nameVector, null);
                }
            } finally {
                memoryManager.releaseMemory(memoryNeeded);
            }

        } catch (IOException e) {
            throw new DataManagerException("Error updating data for key: " + Arrays.toString(key), e);
        } finally {
            dataFileLock.writeLock().unlock();
        }
    }

    public void delete(byte[] key) {
        if (key == null || key.length == 0) {
            throw new DataManagerException("Key cannot be null or empty");
        }

        dataFileLock.writeLock().lock();
        try {
            HeaderRecord header = headerIndex.get(key);
            if (header != null) {
                // Помечаем на удаление в Header Index
                HeaderRecord deletedHeader = new HeaderRecord(false,
                        System.currentTimeMillis() / 1000,
                        header.name, header.dataAddress, header.dataSize, header.nameVector);
                headerIndex.put(key, deletedHeader);

                // Помечаем в кэше
                cache.remove(key);

                // Помечаем в данных
                markDataBlockForDeletion(header.dataAddress);

                // Добавляем в менеджер свободного пространства
                freeSpaceManager.addFreeBlock(header.dataAddress,
                        calculateTotalBlockSize(header.name, new byte[header.dataSize], null));
            }
        } catch (IOException e) {
            throw new DataManagerException("Error deleting data for key: " + Arrays.toString(key), e);
        } finally {
            dataFileLock.writeLock().unlock();
        }
    }

    public List<byte[]> find(SearchQuery query) {
        if (query == null) {
            throw new DataManagerException("Search query cannot be null");
        }

        List<byte[]> results = new ArrayList<>();

        dataFileLock.readLock().lock();
        try {
            // Поиск в Header Index
            for (Map.Entry<HeaderIndex.ByteArrayWrapper, HeaderRecord> entry : headerIndex.entrySet()) {
                HeaderRecord header = entry.getValue();

                if (!header.isActive || isExpired(header.expiryTime)) {
                    continue;
                }

                if (matchesSearchQuery(header, query)) {
                    byte[] data = get(header.name); // Используем get для кэширования
                    if (data != null) {
                        results.add(data);
                    }
                }
            }

        } finally {
            dataFileLock.readLock().unlock();
        }

        return results;
    }

    // Вспомогательные методы

    private boolean isExpired(long expiryTime) {
        return expiryTime != -1 && expiryTime < (System.currentTimeMillis() / 1000);
    }

    private byte[] readDataBlock(HeaderRecord header) throws IOException {
        dataFileLock.readLock().lock();
        try {
            dataFile.seek(header.dataAddress);

            // Чтение дублированного заголовка
            boolean isActive = dataFile.readBoolean();
            long expiryTime = dataFile.readLong();

            int keyLength = dataFile.readInt();
            byte[] key = new byte[keyLength];
            dataFile.readFully(key);

            // Проверка совпадения заголовков
            if (!Arrays.equals(key, header.name) || isActive != header.isActive) {
                throw new IOException("Header mismatch in data block");
            }

            // Чтение разделителя
            byte[] delimiter = new byte[HEADER_DATA_DELIMITER.length];
            dataFile.readFully(delimiter);
            if (!Arrays.equals(delimiter, HEADER_DATA_DELIMITER)) {
                throw new IOException("Invalid header-data delimiter");
            }

            // Читаем данные
            byte[] data = new byte[header.dataSize];
            dataFile.readFully(data);

            // Пропускаем оставшуюся часть блока (векторы и разделители)
            // В реальной реализации нужно читать до RECORD_END_DELIMITER

            return data;
        } finally {
            dataFileLock.readLock().unlock();
        }
    }

    private void writeDataBlock(byte[] key, byte[] value, long expiryTime,
                                float[] dataVector, long address) throws IOException {
        dataFileLock.writeLock().lock();
        try {
            dataFile.seek(address);

            // Запись дублированного заголовка
            dataFile.writeBoolean(true);
            dataFile.writeLong(expiryTime);
            dataFile.writeInt(key.length);
            dataFile.write(key);

            // Разделитель заголовок-данные
            dataFile.write(HEADER_DATA_DELIMITER);

            // Данные
            dataFile.write(value);

            // Вектор данных (если есть)
            if (dataVector != null) {
                dataFile.write(DATA_VECTOR_DELIMITER);
                dataFile.writeInt(dataVector.length);
                for (float f : dataVector) {
                    dataFile.writeFloat(f);
                }
            }

            // Конец записи
            dataFile.write(RECORD_END_DELIMITER);
        } finally {
            dataFileLock.writeLock().unlock();
        }
    }

    private boolean matchesSearchQuery(HeaderRecord header, SearchQuery query) {
        switch (query.getType()) {
            case EXACT_MATCH:
                return Arrays.equals(header.name, query.getKey());

            case MASK_SEARCH:
                return matchesMask(header.name, query.getMask());

            case VECTOR_SEARCH:
                if (header.nameVector != null && query.getVector() != null) {
                    double similarity = calculateCosineSimilarity(header.nameVector, query.getVector());
                    return similarity >= query.getSimilarityThreshold();
                }
                return false;

            default:
                return false;
        }
    }

    private boolean matchesMask(byte[] data, byte[] mask) {
        if (mask == null) return false;

        // Простая реализация поиска по маске с поддержкой wildcard '?'
        if (data.length != mask.length) return false;

        for (int i = 0; i < data.length; i++) {
            if (mask[i] != (byte) '?' && data[i] != mask[i]) {
                return false;
            }
        }
        return true;
    }

    private double calculateCosineSimilarity(float[] v1, float[] v2) {
        if (v1 == null || v2 == null || v1.length != v2.length) return 0.0;

        double dotProduct = 0;
        double norm1 = 0;
        double norm2 = 0;

        for (int i = 0; i < v1.length; i++) {
            dotProduct += v1[i] * v2[i];
            norm1 += v1[i] * v1[i];
            norm2 += v2[i] * v2[i];
        }

        if (norm1 == 0 || norm2 == 0) return 0.0;
        return dotProduct / (Math.sqrt(norm1) * Math.sqrt(norm2));
    }

    private int calculateTotalBlockSize(byte[] key, byte[] data, float[] dataVector) {
        int size = 0;

        // Дублированный заголовок
        size += 1 + 8 + 4 + key.length;

        // Разделители
        size += HEADER_DATA_DELIMITER.length;
        size += RECORD_END_DELIMITER.length;

        // Данные
        size += data.length;

        // Вектор данных (если есть)
        if (dataVector != null) {
            size += DATA_VECTOR_DELIMITER.length + 4 + dataVector.length * 4;
        }

        return size;
    }

    private long estimateMemoryUsage(byte[] key, byte[] value, float[] nameVector, float[] dataVector) {
        long usage = 0;

        // Основные данные
        if (key != null) usage += key.length;
        if (value != null) usage += value.length;

        // Векторы
        if (nameVector != null) usage += nameVector.length * 4L;
        if (dataVector != null) usage += dataVector.length * 4L;

        // Накладные расходы (объекты, служебные структуры)
        usage += 1024; // +1KB для служебных структур

        return usage;
    }

    private long estimateMemoryUsage(byte[] data, HeaderRecord header) {
        long usage = 0;

        if (data != null) usage += data.length;
        if (header != null && header.name != null) usage += header.name.length;
        if (header != null && header.nameVector != null) usage += header.nameVector.length * 4L;

        // Накладные расходы
        usage += 512; // +0.5KB для служебных структур кэша

        return usage;
    }

    private void markForDeletion(HeaderRecord header) throws IOException {
        HeaderRecord deletedHeader = new HeaderRecord(false,
                System.currentTimeMillis() / 1000,
                header.name, header.dataAddress, header.dataSize, header.nameVector);
        headerIndex.put(header.name, deletedHeader);

        freeSpaceManager.addFreeBlock(header.dataAddress,
                calculateTotalBlockSize(header.name, new byte[header.dataSize], null));
    }

    private void markDataBlockForDeletion(long address) throws IOException {
        dataFileLock.writeLock().lock();
        try {
            dataFile.seek(address);
            dataFile.writeBoolean(false); // Помечаем как неактивный
        } finally {
            dataFileLock.writeLock().unlock();
        }
    }

    private void updateDataBlock(HeaderRecord header, byte[] newData) throws IOException {
        dataFileLock.writeLock().lock();
        try {
            dataFile.seek(header.dataAddress);

            // Перезаписываем только данные, сохраняя заголовок
            // Пропускаем заголовок (isActive + expiryTime + keyLength + key)
            dataFile.skipBytes(1 + 8 + 4 + header.name.length);

            // Пропускаем разделитель
            dataFile.skipBytes(HEADER_DATA_DELIMITER.length);

            // Записываем новые данные
            dataFile.write(newData);
        } finally {
            dataFileLock.writeLock().unlock();
        }
    }

    private void initializeFreeSpaceManager() throws IOException {
        // При инициализации сканируем файл данных для поиска удаленных блоков
        // Это упрощенная реализация - в реальной системе нужно хранить карту свободных блоков
        dataFileLock.readLock().lock();
        try {
            // Сканируем файл для поиска удаленных блоков
            long filePointer = 0;
            while (filePointer < dataFile.length()) {
                dataFile.seek(filePointer);

                try {
                    boolean isActive = dataFile.readBoolean();
                    long expiryTime = dataFile.readLong();

                    int keyLength = dataFile.readInt();
                    byte[] key = new byte[keyLength];
                    dataFile.readFully(key);

                    // Пропускаем разделитель
                    dataFile.skipBytes(HEADER_DATA_DELIMITER.length);

                    int dataSize = dataFile.readInt();

                    // Вычисляем размер блока
                    int blockSize = calculateTotalBlockSize(key, new byte[dataSize], null);

                    // Если блок неактивен, добавляем в свободное пространство
                    if (!isActive) {
                        freeSpaceManager.addFreeBlock(filePointer, blockSize);
                    }

                    // Переходим к следующему блоку
                    filePointer += blockSize;

                } catch (EOFException e) {
                    break; // Достигнут конец файла
                } catch (IOException e) {
                    // Пропускаем поврежденные блоки
                    System.err.println("Warning: Corrupted data block at position " + filePointer);
                    break;
                }
            }
        } finally {
            dataFileLock.readLock().unlock();
        }
    }

    private void startBackgroundTasks() {
        // Сборщик мусора
        backgroundScheduler.scheduleAtFixedRate(this::runGarbageCollector, 1, 1, TimeUnit.HOURS);

        // Дефрагментатор
        backgroundScheduler.scheduleAtFixedRate(this::runDefragmentation, 6, 6, TimeUnit.HOURS);

        // Объединение смежных свободных блоков
        backgroundScheduler.scheduleAtFixedRate(freeSpaceManager::mergeAdjacentBlocks, 30, 30, TimeUnit.MINUTES);

        // Мониторинг памяти
        backgroundScheduler.scheduleAtFixedRate(this::logMemoryStats, 5, 5, TimeUnit.MINUTES);
    }

    // Фоновая обработка

    public void runGarbageCollector() {
        dataFileLock.writeLock().lock();
        try {
            List<byte[]> keysToRemove = new ArrayList<>();
            int removedCount = 0;

            for (Map.Entry<HeaderIndex.ByteArrayWrapper, HeaderRecord> entry : headerIndex.entrySet()) {
                HeaderRecord header = entry.getValue();
                if (!header.isActive && isExpired(header.expiryTime)) {
                    keysToRemove.add(entry.getKey().getData());
                    removedCount++;
                }
            }

            // Удаляем просроченные записи
            for (byte[] key : keysToRemove) {
                headerIndex.remove(key);
            }

            if (removedCount > 0) {
                System.out.println("Garbage collector removed " + removedCount + " expired records");
            }

        } catch (IOException e) {
            System.err.println("Error during garbage collection: " + e.getMessage());
        } finally {
            dataFileLock.writeLock().unlock();
        }
    }

    private void runDefragmentation() {
        dataFileLock.writeLock().lock();
        try {
            // Упрощенная реализация дефрагментации
            // В реальной системе нужно переписывать блоки для устранения фрагментации
            System.out.println("Defragmentation cycle completed - free blocks: " +
                    freeSpaceManager.getFreeSpaceCount() + ", total free space: " +
                    freeSpaceManager.getTotalFreeSpace() + " bytes");
        } finally {
            dataFileLock.writeLock().unlock();
        }
    }

    private void logMemoryStats() {
        double usageRatio = memoryManager.getMemoryUsageRatio();
        if (usageRatio > 0.8) {
            System.out.printf("Memory warning: %.1f%% used (%d/%d bytes)%n",
                    usageRatio * 100, memoryManager.getUsedMemory(), memoryManager.getMaxMemory());
        }
    }

    // Дополнительные методы

    public void setCacheTTL(int seconds) {
        cache.setCacheTTL(seconds);
    }

    public int getIndexSize() {
        return headerIndex.size();
    }

    public long getDataFileSize() throws IOException {
        dataFileLock.readLock().lock();
        try {
            return dataFile.length();
        } finally {
            dataFileLock.readLock().unlock();
        }
    }

    public FreeSpaceManager getFreeSpaceManager() {
        return freeSpaceManager;
    }

    public MemoryManager getMemoryManager() {
        return memoryManager;
    }

    public String getDataFileName() {
        return dataFileName;
    }

    public String getIndexFileName() {
        return indexFileName;
    }

    public SystemStats getSystemStats() throws IOException {
        dataFileLock.readLock().lock();
        try {
            return new SystemStats(
                    headerIndex.size(),
                    getDataFileSize(),
                    freeSpaceManager.getFreeSpaceCount(),
                    freeSpaceManager.getTotalFreeSpace(),
                    memoryManager.getUsedMemory(),
                    memoryManager.getMaxMemory(),
                    memoryManager.getMemoryUsageRatio(),
                    cache.getSize()
            );
        } finally {
            dataFileLock.readLock().unlock();
        }
    }

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

    public void compact() throws IOException {
        dataFileLock.writeLock().lock();
        try {
            // Реализация компрессии/дефрагментации файла данных
            // В реальной системе нужно переписать все активные блоки в новый файл
            // и заменить старый файл новым
            System.out.println("Compaction started...");

            // Временная реализация - просто запускаем сборщик мусора
            runGarbageCollector();
            freeSpaceManager.mergeAdjacentBlocks();

            System.out.println("Compaction completed");
        } finally {
            dataFileLock.writeLock().unlock();
        }
    }

    public void backup(String backupPath) throws IOException {
        dataFileLock.readLock().lock();
        try {
            System.out.println("Starting backup to: " + backupPath);

            // Создаем директорию для бэкапа
            File backupDir = new File(backupPath);
            if (!backupDir.exists()) {
                backupDir.mkdirs();
            }

            // Копируем файл данных
            File dataFileSrc = new File(dataFileName);
            File dataFileDst = new File(backupPath, "data.bin.backup");
            copyFile(dataFileSrc, dataFileDst);

            // Копируем файл индекса
            File indexFileSrc = new File(indexFileName);
            File indexFileDst = new File(backupPath, "index.idx.backup");
            copyFile(indexFileSrc, indexFileDst);

            System.out.println("Backup completed successfully");
        } finally {
            dataFileLock.readLock().unlock();
        }
    }

    private void copyFile(File source, File destination) throws IOException {
        try (FileInputStream fis = new FileInputStream(source);
             FileOutputStream fos = new FileOutputStream(destination)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                fos.write(buffer, 0, bytesRead);
            }
        }
    }

    @Override
    public void close() {
        dataFileLock.writeLock().lock();
        try {
            System.out.println("Closing BinaryDataManager...");

            // Закрываем кэш
            cache.close();

            // Останавливаем фоновые задачи
            backgroundScheduler.shutdown();
            if (!backgroundScheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                backgroundScheduler.shutdownNow();
            }

            // Закрываем индекс
            headerIndex.close();

            // Закрываем файл данных
            dataFile.close();

            // Закрываем менеджер памяти
            if (memoryManager != null) {
                memoryManager.close();
            }

            System.out.println("BinaryDataManager closed successfully");

        } catch (IOException | InterruptedException e) {
            throw new DataManagerException("Error closing BinaryDataManager", e);
        } finally {
            dataFileLock.writeLock().unlock();
        }
    }

    // Статические методы для создания экземпляров с различной конфигурацией

    public static BinaryDataManager createWithDefaultMemory(String dataFile, String indexFile) throws IOException {
        return new BinaryDataManager(dataFile, indexFile, DEFAULT_MEMORY_LIMIT);
    }

    public static BinaryDataManager createWithLargeMemory(String dataFile, String indexFile) throws IOException {
        return new BinaryDataManager(dataFile, indexFile, 10L * 1024 * 1024 * 1024); // 10 GB
    }

    public static BinaryDataManager createWithCustomMemory(String dataFile, String indexFile, long memoryBytes) throws IOException {
        if (memoryBytes > MAX_MEMORY_LIMIT) {
            throw new IllegalArgumentException("Memory limit cannot exceed " + MAX_MEMORY_LIMIT + " bytes");
        }
        return new BinaryDataManager(dataFile, indexFile, memoryBytes);
    }
    public DataCache getCache() {
        return cache;
    }
    // Новые методы для SQL операций
    public SqlResult executeSql(SqlQuery query) {
        return sqlProcessor.execute(query);
    }

    public SqlProcessor getSqlProcessor() {
        return sqlProcessor;
    }
}