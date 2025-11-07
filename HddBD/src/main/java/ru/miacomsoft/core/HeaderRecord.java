package ru.miacomsoft.core;

import java.util.Arrays;
import java.util.Objects;

public class HeaderRecord {
    public boolean isActive;
    public long expiryTime;
    public byte[] name;
    public long dataAddress;
    public int dataSize;
    public float[] nameVector;
    public long timestamp;

    public HeaderRecord(boolean isActive, long expiryTime, byte[] name,
                        long dataAddress, int dataSize, float[] nameVector) {
        this.isActive = isActive;
        this.expiryTime = expiryTime;
        this.name = name;
        this.dataAddress = dataAddress;
        this.dataSize = dataSize;
        this.nameVector = nameVector;
        this.timestamp = System.currentTimeMillis();
    }

    // Сериализация заголовка в байты
    public byte[] serialize() {
        int vectorSize = nameVector != null ? nameVector.length : 0;
        int size = 1 + 8 + 4 + name.length + 8 + 4 + 4 + vectorSize * 4;
        byte[] result = new byte[size];
        int offset = 0;

        // isActive
        result[offset++] = (byte) (isActive ? 1 : 0);

        // expiryTime
        writeLong(result, offset, expiryTime);
        offset += 8;

        // name length + name
        writeInt(result, offset, name.length);
        offset += 4;
        System.arraycopy(name, 0, result, offset, name.length);
        offset += name.length;

        // dataAddress
        writeLong(result, offset, dataAddress);
        offset += 8;

        // dataSize
        writeInt(result, offset, dataSize);
        offset += 4;

        // nameVector
        writeInt(result, offset, vectorSize);
        offset += 4;
        if (nameVector != null) {
            for (float f : nameVector) {
                writeFloat(result, offset, f);
                offset += 4;
            }
        }

        return result;
    }

    public static HeaderRecord deserialize(byte[] data) {
        int offset = 0;

        // isActive
        boolean isActive = data[offset++] == 1;

        // expiryTime
        long expiryTime = readLong(data, offset);
        offset += 8;

        // name
        int nameLength = readInt(data, offset);
        offset += 4;
        byte[] name = new byte[nameLength];
        System.arraycopy(data, offset, name, 0, nameLength);
        offset += nameLength;

        // dataAddress
        long dataAddress = readLong(data, offset);
        offset += 8;

        // dataSize
        int dataSize = readInt(data, offset);
        offset += 4;

        // nameVector
        int vectorSize = readInt(data, offset);
        offset += 4;
        float[] nameVector = null;
        if (vectorSize > 0) {
            nameVector = new float[vectorSize];
            for (int i = 0; i < vectorSize; i++) {
                nameVector[i] = readFloat(data, offset);
                offset += 4;
            }
        }

        return new HeaderRecord(isActive, expiryTime, name, dataAddress, dataSize, nameVector);
    }

    private static void writeLong(byte[] data, int offset, long value) {
        for (int i = 0; i < 8; i++) {
            data[offset + i] = (byte) (value >>> (56 - i * 8));
        }
    }

    private static long readLong(byte[] data, int offset) {
        long result = 0;
        for (int i = 0; i < 8; i++) {
            result = (result << 8) | (data[offset + i] & 0xFF);
        }
        return result;
    }

    private static void writeInt(byte[] data, int offset, int value) {
        for (int i = 0; i < 4; i++) {
            data[offset + i] = (byte) (value >>> (24 - i * 8));
        }
    }

    private static int readInt(byte[] data, int offset) {
        int result = 0;
        for (int i = 0; i < 4; i++) {
            result = (result << 8) | (data[offset + i] & 0xFF);
        }
        return result;
    }

    private static void writeFloat(byte[] data, int offset, float value) {
        int intBits = Float.floatToIntBits(value);
        writeInt(data, offset, intBits);
    }

    private static float readFloat(byte[] data, int offset) {
        int intBits = readInt(data, offset);
        return Float.intBitsToFloat(intBits);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HeaderRecord that = (HeaderRecord) o;
        return isActive == that.isActive &&
                expiryTime == that.expiryTime &&
                dataAddress == that.dataAddress &&
                dataSize == that.dataSize &&
                Arrays.equals(name, that.name) &&
                Arrays.equals(nameVector, that.nameVector);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isActive, expiryTime, dataAddress, dataSize,
                Arrays.hashCode(name), Arrays.hashCode(nameVector));
    }
}