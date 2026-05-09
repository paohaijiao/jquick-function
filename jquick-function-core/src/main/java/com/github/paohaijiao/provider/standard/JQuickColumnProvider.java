package com.github.paohaijiao.provider.standard;

import com.github.paohaijiao.core.constant.JQuickStandardProviderConstants;
import com.github.paohaijiao.statement.JQuickRow;
import com.github.paohaijiao.transform.standard.JQuickAbstractJQuickValueProvider;

import java.util.function.Function;

/**
 * 单字段提取 Provider
 */
public class JQuickColumnProvider<T> extends JQuickAbstractJQuickValueProvider<JQuickRow, T> {

    protected final String sourceColumn;

    public JQuickColumnProvider(String sourceColumn, String targetField, Class<T> targetClass) {
        super(targetField, targetClass);
        this.sourceColumn = sourceColumn;
    }

    public JQuickColumnProvider(String sourceColumn, String targetField, Class<T> targetClass, Function<Object, T> converter) {
        super(targetField, targetClass);
        this.sourceColumn = sourceColumn;
        this.converter = converter;
    }

    @Override
    protected Object getRawValue(JQuickRow row) {
        return row.get(sourceColumn);
    }

    public static <T> JQuickColumnProvider<T> of(String sourceColumn, String targetField, Class<T> targetClass) {
        return new JQuickColumnProvider<>(sourceColumn, targetField, targetClass);
    }

    public static JQuickColumnProvider<String> asString(String sourceColumn, String targetField) {
        return new JQuickColumnProvider<>(sourceColumn, targetField, String.class);
    }

    public static JQuickColumnProvider<Integer> asInt(String sourceColumn, String targetField) {
        return new JQuickColumnProvider<>(sourceColumn, targetField, Integer.class);
    }

    public static JQuickColumnProvider<Long> asLong(String sourceColumn, String targetField) {
        return new JQuickColumnProvider<>(sourceColumn, targetField, Long.class);
    }

    public static JQuickColumnProvider<Double> asDouble(String sourceColumn, String targetField) {
        return new JQuickColumnProvider<>(sourceColumn, targetField, Double.class);
    }

    public static JQuickColumnProvider<Boolean> asBoolean(String sourceColumn, String targetField) {
        return new JQuickColumnProvider<>(sourceColumn, targetField, Boolean.class);
    }

    @Override
    public String getName() {
        return JQuickStandardProviderConstants.COLUMN;
    }
}
