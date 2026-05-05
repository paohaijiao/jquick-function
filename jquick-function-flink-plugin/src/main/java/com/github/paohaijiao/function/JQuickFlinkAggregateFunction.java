package com.github.paohaijiao.function;

import com.github.paohaijiao.context.JQuickFunctionContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink聚合函数接口
 *
 * @param <I> 输入类型
 * @param <O> 输出类型
 */
@FunctionalInterface
public interface JQuickFlinkAggregateFunction<I, O> extends JQuickFunction<I, O> {

    /**
     * 执行Flink聚合（流处理）
     *
     * @param env     StreamExecutionEnvironment
     * @param input   输入参数
     * @param context 上下文
     * @return 聚合结果
     */
    O aggregate(StreamExecutionEnvironment env, I input, JQuickFunctionContext context);

    /**
     * 执行Flink聚合（Table API）
     *
     * @param tableEnv TableEnvironment
     * @param input    输入参数
     * @param context  上下文
     * @return 聚合结果
     */
    default O aggregate(TableEnvironment tableEnv, I input, JQuickFunctionContext context) {
        throw new UnsupportedOperationException("Table API not supported");
    }

    @Override
    default O apply(I input) {
        throw new UnsupportedOperationException("Use aggregate() method with Flink environment instead");
    }
}
