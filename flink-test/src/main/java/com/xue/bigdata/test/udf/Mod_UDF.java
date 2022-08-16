package com.xue.bigdata.test.udf;

import org.apache.flink.table.functions.ScalarFunction;

public class Mod_UDF extends ScalarFunction {
    public int eval(Long id, int remainder) {
        return (int) (id % remainder);
    }
}
