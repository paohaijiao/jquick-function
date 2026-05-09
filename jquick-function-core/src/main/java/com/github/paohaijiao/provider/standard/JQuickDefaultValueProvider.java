package com.github.paohaijiao.provider.standard;

import com.github.paohaijiao.core.constant.JQuickStandardProviderConstants;
import com.github.paohaijiao.statement.JQuickRow;
import com.github.paohaijiao.transform.standard.JQuickAbstractJQuickValueProvider;

/**
 * 默认值 Provider
 */
public class JQuickDefaultValueProvider<T> extends JQuickAbstractJQuickValueProvider<JQuickRow, T> {

    private final String sourceColumn;

    public JQuickDefaultValueProvider(String sourceColumn, String targetField, Class<T> targetClass, T defaultValue) {
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

    @Override
    public String getName() {
        return JQuickStandardProviderConstants.DEFAULT;
    }
}