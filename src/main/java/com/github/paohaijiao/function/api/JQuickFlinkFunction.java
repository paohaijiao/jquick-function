package com.github.paohaijiao.function.api;

import com.github.paohaijiao.function.JQuickFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@FunctionalInterface
public interface JQuickFlinkFunction<I, O> extends JQuickFunction<I, O> {

    O run(StreamExecutionEnvironment env, I input) throws Exception;

    @Override
    default O apply(I input) {
        throw new UnsupportedOperationException("Use run(env, input) instead");
    }
}