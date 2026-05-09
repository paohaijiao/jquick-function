package com.github.paohaijiao.transform.standard;


import java.util.function.Function;

/**
 * 转换后的 Provider 包装器
 */
public abstract class JQuickTransformedProvider<T, R, NR> extends JQuickAbstractJQuickValueProvider<T, NR> {

    private final JQuickAbstractJQuickValueProvider<T, R> source;

    private final Function<R, NR> transformer;

    protected JQuickTransformedProvider(JQuickAbstractJQuickValueProvider<T, R> source, Function<R, NR> transformer) {
        super(source.targetField, null);  // 类型会动态确定
        this.source = source;
        this.transformer = transformer;
    }

    @Override
    protected Object getRawValue(T row) {
        R value = source.apply(row);
        return value;
    }

    @Override
    protected NR convert(Object value) {
        if (value == null) return null;
        @SuppressWarnings("unchecked")
        R typed = (R) value;
        return transformer.apply(typed);
    }

}
