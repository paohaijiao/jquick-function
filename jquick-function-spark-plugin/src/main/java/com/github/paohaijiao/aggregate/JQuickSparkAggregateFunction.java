package com.github.paohaijiao.aggregate;


import com.github.paohaijiao.context.JQuickFunctionContext;
import com.github.paohaijiao.function.JQuickFunction;
import org.apache.spark.sql.SparkSession;

/**
 * Spark聚合函数接口
 *
 * @param <I> 输入类型
 * @param <O> 输出类型
 */
@FunctionalInterface
public interface JQuickSparkAggregateFunction<I, O> extends JQuickFunction<I, O> {

    /**
     * 执行Spark聚合
     *
     * @param spark   SparkSession
     * @param input   输入参数
     * @param context 上下文
     * @return 聚合结果
     */
    O aggregate(SparkSession spark, I input, JQuickFunctionContext context);

    @Override
    default O apply(I input) {
        throw new UnsupportedOperationException("Use aggregate() method with SparkSession instead");
    }
}