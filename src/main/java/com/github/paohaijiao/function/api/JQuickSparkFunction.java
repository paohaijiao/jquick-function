package com.github.paohaijiao.function.api;

import com.github.paohaijiao.function.JQuickFunction;
import org.apache.spark.api.java.JavaSparkContext;

@FunctionalInterface
public interface JQuickSparkFunction<I, O> extends JQuickFunction<I, O> {

    O run(JavaSparkContext sc, I input);

    @Override
    default O apply(I input) {
        throw new UnsupportedOperationException("Use run() instead");
    }
}
