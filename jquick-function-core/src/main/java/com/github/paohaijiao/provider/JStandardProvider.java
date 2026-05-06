package com.github.paohaijiao.provider;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 标准字段映射函数接口 - 根据传入的字段值生成新字段
 * 区别于 JQuickAggregationProvider（多行聚合为一行），
 * 本接口用于单行记录的字段转换/派生计算
 *
 * @param <T> 输入参数类型（原始字段值类型）
 * @param <R> 输出结果类型（新字段值类型）
 */
public interface JStandardProvider<T, R> extends JQuickFunctionProvider<T, R> {

    /**
     * 依赖的源字段列表
     * 用于声明该函数计算新字段时需要哪些原始字段
     *
     * @return 依赖字段名列表，默认空列表
     */
    default List<String> getDependentColumns() {
        return Collections.emptyList();
    }

    /**
     * 根据传入的值计算新字段值
     * 输入值列表的顺序应与 getDependentColumns() 返回的字段顺序一致
     *
     * @param values 原始字段值列表
     * @return 计算结果（新字段值）
     */
    @Override
    R apply(List<T> values);

    /**
     * 单值映射便捷方法（只依赖一个字段）
     *
     * @param value 单个原始值
     * @return 计算结果
     */
    default R applyOne(T value) {
        return apply(Collections.singletonList(value));
    }

    /**
     * 双值映射便捷方法（依赖两个字段）
     *
     * @param value1 字段1的值
     * @param value2 字段2的值
     * @return 计算结果
     */
    default R applyTwo(T value1, T value2) {
        return apply(Arrays.asList(value1, value2));
    }

    /**
     * 三值映射便捷方法（依赖三个字段）
     *
     * @param value1 字段1的值
     * @param value2 字段2的值
     * @param value3 字段3的值
     * @return 计算结果
     */
    default R applyThree(T value1, T value2, T value3) {
        return apply(Arrays.asList(value1, value2, value3));
    }

    /**
     * 输出的新字段名称
     *
     * @return 输出字段名，默认返回null由调用方指定
     */
    default String getOutputColumnName() {
        return null;
    }

    /**
     * 当输入值为null时的处理策略
     *
     * @return true: 跳过返回null；false: 继续处理
     */
    default boolean skipOnNull() {
        return true;
    }
}