package com.github.paohaijiao.provider.standard.impl;

import com.github.paohaijiao.provider.standard.JQuickAbstractJQuickValueProvider;
import com.github.paohaijiao.statement.JQuickRow;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * 条件 Provider
 */
public class JQuickConditionalProvider<T> extends JQuickAbstractJQuickValueProvider<JQuickRow, T> {

    private final List<Condition<T>> conditions = new ArrayList<>();

    public JQuickConditionalProvider(String targetField, Class<T> targetClass) {
        super(targetField, targetClass);
    }

    public JQuickConditionalProvider(String targetField, Class<T> targetClass, T defaultValue) {
        super(targetField, targetClass);
        this.defaultValue = defaultValue;
        this.nullable = true;
    }

    public JQuickConditionalProvider<T> when(Function<JQuickRow, Boolean> predicate, T value) {
        conditions.add(new Condition<>(predicate, value));
        return this;
    }

    public JQuickConditionalProvider<T> whenEquals(String column, Object expected, T value) {
        return when(row -> Objects.equals(row.get(column), expected), value);
    }

    @Override
    protected Object getRawValue(JQuickRow row) {
        for (Condition<T> condition : conditions) {
            if (condition.predicate.apply(row)) {
                return condition.value;
            }
        }
        return defaultValue;
    }

    private static class Condition<T> {
        final Function<JQuickRow, Boolean> predicate;
        final T value;

        Condition(Function<JQuickRow, Boolean> predicate, T value) {
            this.predicate = predicate;
            this.value = value;
        }
    }
}
