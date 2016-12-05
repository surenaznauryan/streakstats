package com.aznauryan.exception;

/**
 * Created by Suren on 12/5/2016.
 */
public class IllegalStatusException extends RuntimeException {

    public IllegalStatusException(String message) {
        super(message);
    }

    public IllegalStatusException(String message, Throwable cause) {
        super(message, cause);
    }
}
