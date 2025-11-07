package ru.miacomsoft.core;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class HeaderIndex {
    private final ConcurrentSkipListMap<ByteArrayWrapper, HeaderRecord> index;
    private final String indexFilePath;
    private final RandomAccessFile indexFile;

    public static class ByteArrayWrapper implements Comparable<ByteArrayWrapper> {
        private final byte[] data;

        public ByteArrayWrapper(byte[] data) {
            this.data = data;
        }

        @Override
        public int compareTo(ByteArrayWrapper other) {
            return Arrays.compare(this.data, other.data);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            ByteArrayWrapper that = (ByteArrayWrapper) obj;
            return Arrays.equals(data, that.data);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(data);
        }

        public byte[] getData() {
            return data;
        }
    }

    public HeaderIndex(String indexFilePath) throws IOException {
        this.indexFilePath = indexFilePath;
        this.index = new ConcurrentSkipListMap<>();

        File file = new File(indexFilePath);
        boolean fileExists = file.exists();

        this.indexFile = new RandomAccessFile(file, "rw");

        if (fileExists) {
            loadFromFile();
        }
    }

    public void put(byte[] key, HeaderRecord record) throws IOException {
        index.put(new ByteArrayWrapper(key), record);
        saveToFile();
    }

    public HeaderRecord get(byte[] key) {
        ByteArrayWrapper wrapper = new ByteArrayWrapper(key);
        return index.get(wrapper);
    }

    public void remove(byte[] key) throws IOException {
        index.remove(new ByteArrayWrapper(key));
        saveToFile();
    }

    public boolean containsKey(byte[] key) {
        return index.containsKey(new ByteArrayWrapper(key));
    }

    public Collection<HeaderRecord> getAllRecords() {
        return index.values();
    }

    public Set<Map.Entry<ByteArrayWrapper, HeaderRecord>> entrySet() {
        return index.entrySet();
    }

    private void loadFromFile() throws IOException {
        indexFile.seek(0);

        while (indexFile.getFilePointer() < indexFile.length()) {
            // Читаем длину записи
            int recordLength = indexFile.readInt();
            if (recordLength <= 0) break;

            // Читаем данные записи
            byte[] recordData = new byte[recordLength];
            indexFile.readFully(recordData);

            HeaderRecord record = HeaderRecord.deserialize(recordData);
            if (record != null) {
                index.put(new ByteArrayWrapper(record.name), record);
            }
        }
    }

    private void saveToFile() throws IOException {
        indexFile.setLength(0); // Очищаем файл

        for (HeaderRecord record : index.values()) {
            byte[] recordData = record.serialize();

            // Записываем длину записи и сами данные
            indexFile.writeInt(recordData.length);
            indexFile.write(recordData);
        }
    }

    public void close() throws IOException {
        saveToFile();
        indexFile.close();
    }

    public int size() {
        return index.size();
    }
}