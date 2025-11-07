package ru.miacomsoft.core;

public class SearchQuery {
    public enum SearchType {
        EXACT_MATCH,
        MASK_SEARCH,
        VECTOR_SEARCH
    }

    private final SearchType type;
    private final byte[] key;
    private final byte[] mask;
    private final float[] vector;
    private final double similarityThreshold;
    private final byte[] searchNode;

    private SearchQuery(Builder builder) {
        this.type = builder.type;
        this.key = builder.key;
        this.mask = builder.mask;
        this.vector = builder.vector;
        this.similarityThreshold = builder.similarityThreshold;
        this.searchNode = builder.searchNode;
    }

    public static class Builder {
        private SearchType type;
        private byte[] key;
        private byte[] mask;
        private float[] vector;
        private double similarityThreshold = 0.8;
        private byte[] searchNode;

        public Builder exactMatch(byte[] key) {
            this.type = SearchType.EXACT_MATCH;
            this.key = key;
            return this;
        }

        public Builder maskSearch(byte[] mask) {
            this.type = SearchType.MASK_SEARCH;
            this.mask = mask;
            return this;
        }

        public Builder vectorSearch(float[] vector, double threshold) {
            this.type = SearchType.VECTOR_SEARCH;
            this.vector = vector;
            this.similarityThreshold = threshold;
            return this;
        }

        public Builder withSearchNode(byte[] node) {
            this.searchNode = node;
            return this;
        }

        public SearchQuery build() {
            return new SearchQuery(this);
        }
    }

    // Getters
    public SearchType getType() { return type; }
    public byte[] getKey() { return key; }
    public byte[] getMask() { return mask; }
    public float[] getVector() { return vector; }
    public double getSimilarityThreshold() { return similarityThreshold; }
    public byte[] getSearchNode() { return searchNode; }
}