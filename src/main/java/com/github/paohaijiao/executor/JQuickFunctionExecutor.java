package com.github.paohaijiao.executor;

import com.github.paohaijiao.context.JQuickFunctionContext;
import com.github.paohaijiao.function.JQuickFunction;

public interface JQuickFunctionExecutor {

    /**
     * 是否支持该函数
     */
    boolean supports(JQuickFunction<?, ?> function);

    /**
     * 执行函数
     */
    <I, O> O execute(JQuickFunction<I, O> function, I input, JQuickFunctionContext context);

    /**
     * 执行引擎类型
     */
    String engine(); // java / spark / flink
}
