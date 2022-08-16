package com.xue.bigdata.utils;

/**
 * @author: mingway
 * @date: 2022/8/12 11:31 PM
 */
@FunctionalInterface
public interface ThrowableSupplier<OUT, EXCEPTION extends Throwable> {

    OUT get() throws EXCEPTION;

}
