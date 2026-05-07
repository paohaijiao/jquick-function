package com.github.paohaijiao.provider;


import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * 聚合函数提供者接口 - 专为 Spark 聚合设计
 *
 * @param <T> 输入参数类型
 * @param <R> 聚合结果类型
 */
public interface JQuickFunctionProvider<T, R> extends Serializable {
    /**
     * 依赖的原始字段 ， 可能是多个
     * 用于声明该函数计算新字段时需要哪些原始字段
     *
     * @return 依赖字段名列表，默认空列表
     */
    default List<String> getSourceField() {
        return Collections.emptyList();
    }




    public R apply(T t);

    /**
     * 转换后的字段
     * @return
     */
    public String getTargetField() ;

    /**
     *
     * @return
     */
    public Class<?> getTargetClass() ;

    /**
     * 初始化方法（可选实现）
     */
    default void init() {
    }
    /**
     * 结束方法（可选实现）
     */
    default void destroy() {
    }


}