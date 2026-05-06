package com.github.paohaijiao.provider.standard.impl;


import com.github.paohaijiao.compute.JQuickComputeTypeImpl;
import com.github.paohaijiao.provider.standard.JQuickBaseStandardProvider;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 将多个字段拼接成字符串
 */
public class ConcatProvider extends JQuickBaseStandardProvider<String> {

    private final String delimiter;

    public ConcatProvider(List<String> dependentColumns, String outputColumnName, String delimiter) {
        super(dependentColumns, outputColumnName);
        this.delimiter = delimiter != null ? delimiter : "";
    }

    @Override
    protected String transform(List<Object> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        return values.stream()
                .map(v -> v != null ? v.toString() : "")
                .filter(s -> !s.isEmpty())
                .collect(Collectors.joining(delimiter));
    }

    @Override
    public JQuickComputeTypeImpl getType() {
        return null;
    }
}
