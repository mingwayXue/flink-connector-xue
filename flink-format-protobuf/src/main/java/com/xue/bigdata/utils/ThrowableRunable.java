package com.xue.bigdata.utils;

/**
 * @author: mingway
 * @date: 2022/8/12 11:33 PM
 */
@FunctionalInterface
public interface ThrowableRunable<EXCEPTION extends Throwable> {

    void run() throws EXCEPTION;

}
