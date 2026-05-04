package com.github.paohaijiao.function;

@FunctionalInterface
public interface JQuickFunction<I, O> {

    O apply(I input);

    default String name() {
        return this.getClass().getSimpleName();
    }
}
