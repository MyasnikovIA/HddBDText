package ru.miacomsoft.core;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

public class FreeSpaceManager {
    private final NavigableSet<FreeBlock> freeBlocks;

    private static class FreeBlock implements Comparable<FreeBlock> {
        final long address;
        final int size;

        FreeBlock(long address, int size) {
            this.address = address;
            this.size = size;
        }

        @Override
        public int compareTo(FreeBlock other) {
            int sizeCompare = Integer.compare(this.size, other.size);
            if (sizeCompare != 0) return sizeCompare;
            return Long.compare(this.address, other.address);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FreeBlock freeBlock = (FreeBlock) o;
            return address == freeBlock.address && size == freeBlock.size;
        }

        @Override
        public int hashCode() {
            return Objects.hash(address, size);
        }
    }

    public FreeSpaceManager() {
        this.freeBlocks = new ConcurrentSkipListSet<>();
    }

    public void addFreeBlock(long address, int size) {
        freeBlocks.add(new FreeBlock(address, size));
    }

    public long findFreeSpace(int requiredSize) {
        FreeBlock candidate = freeBlocks.ceiling(new FreeBlock(0, requiredSize));
        if (candidate != null) {
            freeBlocks.remove(candidate);

            // Если осталось свободное место, добавляем обратно остаток
            int remaining = candidate.size - requiredSize;
            if (remaining > 0) {
                freeBlocks.add(new FreeBlock(candidate.address + requiredSize, remaining));
            }

            return candidate.address;
        }
        return -1; // Свободного места нет
    }

    public void mergeAdjacentBlocks() {
        if (freeBlocks.isEmpty()) return;

        List<FreeBlock> sortedByAddress = new ArrayList<>(freeBlocks);
        sortedByAddress.sort(Comparator.comparingLong(b -> b.address));

        List<FreeBlock> merged = new ArrayList<>();
        FreeBlock current = sortedByAddress.get(0);

        for (int i = 1; i < sortedByAddress.size(); i++) {
            FreeBlock next = sortedByAddress.get(i);

            if (current.address + current.size == next.address) {
                // Блоки смежные, объединяем
                current = new FreeBlock(current.address, current.size + next.size);
            } else {
                merged.add(current);
                current = next;
            }
        }
        merged.add(current);

        freeBlocks.clear();
        freeBlocks.addAll(merged);
    }

    public int getFreeSpaceCount() {
        return freeBlocks.size();
    }

    public long getTotalFreeSpace() {
        return freeBlocks.stream().mapToLong(block -> block.size).sum();
    }
}