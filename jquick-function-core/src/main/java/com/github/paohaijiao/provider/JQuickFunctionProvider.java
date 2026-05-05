package com.github.paohaijiao.provider;

import com.github.paohaijiao.compute.JQuickComputeTypeImpl;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 聚合函数提供者接口 - 专为 Spark 聚合设计
 *
 * @param <T> 输入参数类型
 * @param <R> 聚合结果类型
 */
public interface JQuickFunctionProvider<T, R> extends Serializable {

    public JQuickComputeTypeImpl getType();


    /**
     * 执行函数
     *
     * @param args 参数列表，统一使用 List<T> 形式
     * @return 执行结果
     */
    R apply(List<T> args);

    /**
     * 初始化方法（可选实现）
     */
    default void init() {
    }

    /**
     * 销毁方法（可选实现）
     */
    default void destroy() {
    }

    /**
     * 单参数便捷方法
     */
    default R applyOne(T arg) {
        return apply(Collections.singletonList(arg));
    }

    /**
     * 双参数便捷方法
     */
    default R applyTwo(T arg1, T arg2) {
        return apply(Arrays.asList(arg1, arg2));
    }

    /**
     * 三参数便捷方法
     */
    default R applyThree(T arg1, T arg2, T arg3) {
        return apply(Arrays.asList(arg1, arg2, arg3));
    }

}