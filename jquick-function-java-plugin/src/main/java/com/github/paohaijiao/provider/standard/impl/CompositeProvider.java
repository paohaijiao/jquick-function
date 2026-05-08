package com.github.paohaijiao.provider.standard.impl;


import com.github.paohaijiao.provider.standard.JQuickAbstractJQuickValueProvider;
import com.github.paohaijiao.statement.JQuickRow;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * 组合 Provider（多字段）
 */
public class CompositeProvider<T> extends JQuickAbstractJQuickValueProvider<JQuickRow, T> {

    protected final Function<JQuickRow, T> combiner;

    protected final List<String> dependentColumns;

    public CompositeProvider(String targetField, Class<T> targetClass, Function<JQuickRow, T> combiner, String... dependentColumns) {
        super(targetField, targetClass);
        this.combiner = combiner;
        this.dependentColumns = Arrays.asList(dependentColumns);
    }

    @Override
    protected Object getRawValue(JQuickRow row) {
        return combiner.apply(row);
    }

    public List<String> getDependentColumns() {
        return dependentColumns;
    }

    public static CompositeProvider<String> concat(String targetField, String delimiter, String... columns) {
        return new CompositeProvider<>(targetField, String.class, row -> {
            StringBuilder sb = new StringBuilder();
            for (String col : columns) {
                Object val = row.get(col);
                if (val != null) {
                    if (sb.length() > 0) sb.append(delimiter);
                    sb.append(val.toString());
                }
            }
            return sb.toString();
        }, columns);
    }

    public static CompositeProvider<Double> sum(String targetField, String... numericColumns) {
        return new CompositeProvider<>(targetField, Double.class, row -> {
            double sum = 0.0;
            for (String col : numericColumns) {
                Number val = row.getAs(col, Number.class);
                if (val != null) sum += val.doubleValue();
            }
            return sum;
        }, numericColumns);
    }
}
