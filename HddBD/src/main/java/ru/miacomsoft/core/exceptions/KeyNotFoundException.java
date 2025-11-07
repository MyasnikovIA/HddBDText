package ru.miacomsoft.core.exceptions;

import java.util.Arrays;

public class KeyNotFoundException extends DataManagerException {
    public KeyNotFoundException(String message) {
        super(message);
    }

    public KeyNotFoundException(byte[] key) {
        super("Key not found: " + Arrays.toString(key));
    }
}