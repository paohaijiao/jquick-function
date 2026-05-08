package com.github.paohaijiao.provider.standard.impl;
import com.github.paohaijiao.provider.standard.JQuickAbstractJQuickValueProvider;
import com.github.paohaijiao.statement.JQuickRow;

/**
 * 默认值 Provider
 */
public class DefaultValueProvider<T> extends JQuickAbstractJQuickValueProvider<JQuickRow, T> {

    private final String sourceColumn;

    public DefaultValueProvider(String sourceColumn, String targetField, Class<T> targetClass, T defaultValue) {
        super(targetField, targetClass);
        this.sourceColumn = sourceColumn;
        this.defaultValue = defaultValue;
        this.nullable = true;
    }

    @Override
    protected Object getRawValue(JQuickRow row) {
        Object value = row.get(sourceColumn);
        return value != null ? value : defaultValue;
    }

    @Override
    protected T handleNull() {
        return defaultValue;
    }
}