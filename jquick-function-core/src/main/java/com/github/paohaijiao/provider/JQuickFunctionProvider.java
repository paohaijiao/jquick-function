package com.github.paohaijiao.provider;
import java.io.Serializable;
/**
 * 聚合函数提供者接口 - 专为 Spark 聚合设计
 *
 * @param <T> 输入参数类型
 * @param <R> 聚合结果类型
 */
public interface JQuickFunctionProvider<T, R> extends Serializable {




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


}