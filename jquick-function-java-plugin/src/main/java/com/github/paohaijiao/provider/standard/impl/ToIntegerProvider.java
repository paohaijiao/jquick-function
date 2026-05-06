package com.github.paohaijiao.provider.standard.impl;

import com.github.paohaijiao.compute.JQuickComputeTypeImpl;
import com.github.paohaijiao.provider.standard.JQuickBaseStandardProvider;

import java.util.List;

/**
 * 将单个字段转换为 Integer 类型
 */
public class ToIntegerProvider extends JQuickBaseStandardProvider<Integer> {

    public ToIntegerProvider(String dependentColumn, String outputColumnName) {
        super(dependentColumn, outputColumnName);
    }

    @Override
    protected Integer transform(List<Object> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        Object value = values.get(0);
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        if (value instanceof String) {
            try {
                return Integer.parseInt((String) value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        if (value instanceof Boolean) {
            return ((Boolean) value) ? 1 : 0;
        }
        return null;
    }

    @Override
    public JQuickComputeTypeImpl getType() {
        return null;
    }
}