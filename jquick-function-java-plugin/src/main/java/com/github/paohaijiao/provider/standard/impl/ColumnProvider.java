package com.github.paohaijiao.provider.standard.impl;

import com.github.paohaijiao.provider.standard.JQuickAbstractJQuickValueProvider;
import com.github.paohaijiao.statement.JQuickRow;

import java.util.function.Function;

/**
 * 单字段提取 Provider
 */
public class ColumnProvider<T> extends JQuickAbstractJQuickValueProvider<JQuickRow, T> {

    protected final String sourceColumn;

    public ColumnProvider(String sourceColumn, String targetField, Class<T> targetClass) {
        super(targetField, targetClass);
        this.sourceColumn = sourceColumn;
    }

    public ColumnProvider(String sourceColumn, String targetField,
                          Class<T> targetClass, Function<Object, T> converter) {
        super(targetField, targetClass);
        this.sourceColumn = sourceColumn;
        this.converter = converter;
    }

    @Override
    protected Object getRawValue(JQuickRow row) {
        return row.get(sourceColumn);
    }

    public static <T> ColumnProvider<T> of(String sourceColumn, String targetField, Class<T> targetClass) {
        return new ColumnProvider<>(sourceColumn, targetField, targetClass);
    }

    public static ColumnProvider<String> asString(String sourceColumn, String targetField) {
        return new ColumnProvider<>(sourceColumn, targetField, String.class);
    }

    public static ColumnProvider<Integer> asInt(String sourceColumn, String targetField) {
        return new ColumnProvider<>(sourceColumn, targetField, Integer.class);
    }

    public static ColumnProvider<Long> asLong(String sourceColumn, String targetField) {
        return new ColumnProvider<>(sourceColumn, targetField, Long.class);
    }

    public static ColumnProvider<Double> asDouble(String sourceColumn, String targetField) {
        return new ColumnProvider<>(sourceColumn, targetField, Double.class);
    }

    public static ColumnProvider<Boolean> asBoolean(String sourceColumn, String targetField) {
        return new ColumnProvider<>(sourceColumn, targetField, Boolean.class);
    }
}
