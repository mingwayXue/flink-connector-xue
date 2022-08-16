package com.xue.bigdata.utils;

/**
 * @author: mingway
 * @date: 2022/8/12 11:04 PM
 */
public class MoreSuppliers {

    private MoreSuppliers() {
        throw new UnsupportedOperationException();
    }

    public static <OUT> OUT throwing(ThrowableSupplier<OUT, Throwable> throwableSupplier) {
        try {
            return throwableSupplier.get();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}
