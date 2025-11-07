package ru.miacomsoft.core.exceptions;

public class DataManagerException extends RuntimeException {
    public DataManagerException(String message) {
        super(message);
    }

    public DataManagerException(String message, Throwable cause) {
        super(message, cause);
    }
}