package com.xue.bigdata.test.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;

import java.util.Random;

public class CustomDefinedSource implements SourceFunction<Row>, ResultTypeQueryable<Row> {
    private volatile boolean isCancel;

    @Override
    public void run(SourceFunction.SourceContext<Row> sourceContext) throws Exception {

        Random random = new Random();

        while (!this.isCancel) {

            sourceContext.collect(Row.of(random.nextLong(), "Test", "dev"));

            Thread.sleep(3000L);
        }

    }

    @Override
    public void cancel() {
        this.isCancel = true;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return new RowTypeInfo(TypeInformation.of(Long.class), TypeInformation.of(String.class),
                TypeInformation.of(String.class));
    }
}