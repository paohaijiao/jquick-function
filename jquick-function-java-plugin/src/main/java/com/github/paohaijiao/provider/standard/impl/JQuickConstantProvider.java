package com.github.paohaijiao.provider.standard.impl;

import com.github.paohaijiao.provider.standard.JQuickAbstractJQuickValueProvider;
import com.github.paohaijiao.statement.JQuickRow;

/**
 * 常量值 Provider
 */
public class JQuickConstantProvider<T> extends JQuickAbstractJQuickValueProvider<JQuickRow, T> {

    private final T constant;

    public JQuickConstantProvider(String targetField, Class<T> targetClass, T constant) {
        super(targetField, targetClass);
        this.constant = constant;
        this.nullable = constant == null;
        this.defaultValue = constant;
    }

    @Override
    protected Object getRawValue(JQuickRow row) {
        return constant;
    }

    public static JQuickConstantProvider<String> string(String targetField, String value) {
        return new JQuickConstantProvider<>(targetField, String.class, value);
    }

    public static JQuickConstantProvider<Integer> integer(String targetField, Integer value) {
        return new JQuickConstantProvider<>(targetField, Integer.class, value);
    }
}

