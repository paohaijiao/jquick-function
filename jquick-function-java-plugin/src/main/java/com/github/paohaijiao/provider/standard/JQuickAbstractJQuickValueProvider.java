package com.github.paohaijiao.provider.standard;


import com.github.paohaijiao.provider.JQuickFunctionProvider;
import com.github.paohaijiao.statement.JQuickRow;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * JQuickFunctionProvider 抽象基类
 * 提供通用的字段元数据管理和类型转换能力
 *
 * @param <T> 输入参数类型（通常是 JQuickRow）
 * @param <R> 输出结果类型
 * @author Martin
 * @since 1.0.0
 */
public abstract class JQuickAbstractJQuickValueProvider<T, R> implements JQuickFunctionProvider<T, R> {

    /** 目标字段名 */
    protected final String targetField;

    /** 目标类型 */
    protected final Class<R> targetClass;

    /** 是否允许返回 null */
    protected boolean nullable = true;

    /** 当值为 null 时的默认值 */
    protected R defaultValue = null;

    /** 自定义转换器 */
    protected Function<Object, R> converter = null;

    /**
     * 基础构造器
     *
     * @param targetField 目标字段名
     * @param targetClass 目标类型
     */
    protected JQuickAbstractJQuickValueProvider(String targetField, Class<R> targetClass) {
        this.targetField = targetField;
        this.targetClass = targetClass;
    }

    /**
     * 完整构造器
     *
     * @param targetField 目标字段名
     * @param targetClass 目标类型
     * @param nullable 是否允许 null
     * @param defaultValue 默认值
     * @param converter 自定义转换器
     */
    protected JQuickAbstractJQuickValueProvider(String targetField, Class<R> targetClass, boolean nullable, R defaultValue, Function<Object, R> converter) {
        this.targetField = targetField;
        this.targetClass = targetClass;
        this.nullable = nullable;
        this.defaultValue = defaultValue;
        this.converter = converter;
    }

    @Override
    public String getTargetField() {
        return targetField;
    }

    @Override
    public Class<?> getTargetClass() {
        return targetClass;
    }

    /**
     * 核心转换方法 - 子类实现具体的值获取逻辑
     *
     * @param row 输入行数据
     * @return 原始值（可能为 null）
     */
    protected abstract Object getRawValue(T row);

    /**
     * 应用类型转换和默认值处理
     *
     * @param row 输入行数据
     * @return 转换后的值
     */
    @Override
    public R apply(T row) {
        if (row == null) {
            return handleNull();
        }
        Object rawValue = getRawValue(row);
        if (rawValue == null) {
            return handleNull();
        }
        R converted = convert(rawValue);
        if (converted == null && !nullable) {
            throw new IllegalStateException(String.format("Converted value is null for field '%s', but nullable is false", targetField));
        }
        return converted;
    }

    /**
     * 处理 null 值的逻辑
     */
    protected R handleNull() {
        if (!nullable && defaultValue == null) {
            throw new IllegalStateException(String.format("Null value not allowed for field '%s' and no default provided", targetField));
        }
        return defaultValue;
    }

    /**
     * 类型转换核心方法
     *
     * @param value 原始值
     * @return 转换后的值
     */
    @SuppressWarnings("unchecked")
    protected R convert(Object value) {
        if (converter != null) {
            return converter.apply(value);
        }
        if (targetClass.isInstance(value)) {//类型匹配直接返回
            return (R) value;
        }
        return tryStandardConversion(value);//尝试标准类型转换
    }

    /**
     * 标准类型转换尝试
     */
    @SuppressWarnings("unchecked")
    protected R tryStandardConversion(Object value) {
        if (value == null) return null;
        if (targetClass == String.class) {// String 转换
            return (R) value.toString();
        }
        if (value instanceof Number) {// 数字类型转换
            Number num = (Number) value;
            if (targetClass == Integer.class) return (R) Integer.valueOf(num.intValue());
            if (targetClass == Long.class) return (R) Long.valueOf(num.longValue());
            if (targetClass == Double.class) return (R) Double.valueOf(num.doubleValue());
            if (targetClass == Float.class) return (R) Float.valueOf(num.floatValue());
            if (targetClass == Short.class) return (R) Short.valueOf(num.shortValue());
            if (targetClass == Byte.class) return (R) Byte.valueOf(num.byteValue());
        }
        if (targetClass == Boolean.class) { // Boolean 转换
            return (R) convertToBoolean(value);
        }
        if (value instanceof String && Number.class.isAssignableFrom(targetClass)) {// 字符串到数字的转换
            return convertStringToNumber((String) value);
        }
        return null;
    }

    /**
     * 转换为 Boolean
     */
    private Boolean convertToBoolean(Object value) {
        if (value instanceof Boolean) return (Boolean) value;
        if (value instanceof Number) return ((Number) value).intValue() != 0;
        if (value instanceof String) {
            String str = ((String) value).toLowerCase();
            if ("true".equals(str) || "1".equals(str) || "yes".equals(str) || "on".equals(str))
                return true;
            if ("false".equals(str) || "0".equals(str) || "no".equals(str) || "off".equals(str))
                return false;
        }
        return null;
    }

    /**
     * 字符串转数字
     */
    @SuppressWarnings("unchecked")
    private R convertStringToNumber(String str) {
        try {
            if (targetClass == Integer.class)
                return (R) Integer.valueOf(Integer.parseInt(str));
            if (targetClass == Long.class)
                return (R) Long.valueOf(Long.parseLong(str));
            if (targetClass == Double.class)
                return (R) Double.valueOf(Double.parseDouble(str));
            if (targetClass == Float.class)
                return (R) Float.valueOf(Float.parseFloat(str));
        } catch (NumberFormatException e) {
            return null;
        }
        return null;
    }

    /**
     * 设置是否允许 null
     */
    @SuppressWarnings("unchecked")
    public <P extends JQuickAbstractJQuickValueProvider<T, R>> P nullable(boolean nullable) {
        this.nullable = nullable;
        return (P) this;
    }

    /**
     * 设置默认值
     */
    @SuppressWarnings("unchecked")
    public <P extends JQuickAbstractJQuickValueProvider<T, R>> P defaultValue(R defaultValue) {
        this.defaultValue = defaultValue;
        this.nullable = true;  // 有默认值时允许 null
        return (P) this;
    }

    /**
     * 设置自定义转换器
     */
    @SuppressWarnings("unchecked")
    public <P extends JQuickAbstractJQuickValueProvider<T, R>> P converter(Function<Object, R> converter) {
        this.converter = converter;
        return (P) this;
    }

    /**
     * 应用后处理转换
     */
    public <NR> JQuickAbstractJQuickValueProvider<T, NR> andThen(Function<R, NR> after) {
        return new TransformedProvider<>(this, after);
    }


    /**
     * 安全地获取数值（用于聚合计算）
     */
    protected double getNumericValue(Object value, double defaultValue) {
        if (value == null) return defaultValue;
        if (value instanceof Number) return ((Number) value).doubleValue();
        try {
            return Double.parseDouble(value.toString());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * 检查两个 Provider 是否目标字段相同（用于链式操作）
     */
    public boolean isSameTarget(JQuickAbstractJQuickValueProvider<?, ?> other) {
        return other != null && Objects.equals(this.targetField, other.targetField);
    }

    @Override
    public String toString() {
        return String.format("%s{targetField='%s', targetClass=%s, nullable=%s, defaultValue=%s}",
                getClass().getSimpleName(), targetField, targetClass.getSimpleName(), nullable, defaultValue);
    }
}

/**
 * 转换后的 Provider 包装器
 */
class TransformedProvider<T, R, NR> extends JQuickAbstractJQuickValueProvider<T, NR> {

    private final JQuickAbstractJQuickValueProvider<T, R> source;

    private final Function<R, NR> transformer;

    protected TransformedProvider(JQuickAbstractJQuickValueProvider<T, R> source, Function<R, NR> transformer) {
        super(source.targetField, null);  // 类型会动态确定
        this.source = source;
        this.transformer = transformer;
    }

    @Override
    protected Object getRawValue(T row) {
        R value = source.apply(row);
        return value;
    }

    @Override
    protected NR convert(Object value) {
        if (value == null) return null;
        @SuppressWarnings("unchecked")
        R typed = (R) value;
        return transformer.apply(typed);
    }
}



