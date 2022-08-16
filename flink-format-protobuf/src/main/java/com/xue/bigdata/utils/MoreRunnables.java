package com.xue.bigdata.utils;

/**
 * @author: mingway
 * @date: 2022/8/12 11:32 PM
 */
public class MoreRunnables {
    public static <EXCEPTION extends Throwable> void throwing(ThrowableRunable<EXCEPTION> throwableRunable) {
        try {
            throwableRunable.run();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}
