package com.github.paohaijiao.provider;

import java.util.List;

/**
 * 聚合函数接口 - 对一组数据进行聚合操作
 * 继承自 JQuickFunctionProvider，用于聚合场景
 *
 * @param <T> 输入元素类型
 * @param <R> 聚合结果类型
 */
public interface JQuickAggregationProvider<T, R> extends JQuickFunctionProvider<T, R> {


    /**
     * group by 字段
     *
     * @return
     */
    public List<String> getColumns();

    /**
     * 对列表进行聚合
     *
     * @param args 待聚合的数据列表
     * @return 聚合结果
     */
    @Override
    R apply(List<T> args);

    /**
     * 聚合的核心方法（子类实现）
     *
     * @param values 数值列表
     * @return 聚合结果
     */
    R aggregate(List<T> values);

    /**
     * 合并两个聚合结果（用于并行聚合）
     *
     * @param result1 部分聚合结果1
     * @param result2 部分聚合结果2
     * @return 合并后的结果
     */
    default R combine(R result1, R result2) {
        throw new UnsupportedOperationException("Combine not implemented");
    }

    /**
     * 获取初始值（用于累加器）
     */
    default R getInitialValue() {
        return null;
    }

    /**
     * 检查是否支持并行聚合
     */
    default boolean isSupportParallel() {
        return false;
    }
}
