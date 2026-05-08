package com.github.paohaijiao.provider.standard.impl;

import com.github.paohaijiao.provider.standard.JQuickAbstractJQuickValueProvider;
import com.github.paohaijiao.statement.JQuickRow;

/**
 * 常量值 Provider
 */
public class ConstantProvider<T> extends JQuickAbstractJQuickValueProvider<JQuickRow, T> {

    private final T constant;

    public ConstantProvider(String targetField, Class<T> targetClass, T constant) {
        super(targetField, targetClass);
        this.constant = constant;
        this.nullable = constant == null;
        this.defaultValue = constant;
    }

    @Override
    protected Object getRawValue(JQuickRow row) {
        return constant;
    }

    public static ConstantProvider<String> string(String targetField, String value) {
        return new ConstantProvider<>(targetField, String.class, value);
    }

    public static ConstantProvider<Integer> integer(String targetField, Integer value) {
        return new ConstantProvider<>(targetField, Integer.class, value);
    }
}

