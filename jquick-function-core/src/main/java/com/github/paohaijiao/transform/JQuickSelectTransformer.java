package com.github.paohaijiao.transform;

import com.github.paohaijiao.provider.JQuickFunctionProvider;
import com.github.paohaijiao.provider.standard.*;
import com.github.paohaijiao.statement.JQuickColumnMeta;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;

import java.util.*;
import java.util.function.Function;

/**
 * SELECT 转换器 - 从源数据集中选择并转换指定的列
 * 类似于 SQL 的 SELECT 语句，可以：
 * - 选择指定列
 * - 重命名列
 * - 类型转换
 * - 计算新列
 * - 应用表达式/函数
 * - 使用固定值
 *
 * @author Martin
 * @since 1.0.0
 */
public class JQuickSelectTransformer extends JQuickDataSetTransformer {

    /** 是否保留原始列（默认 false，只保留 provider 定义的列） */
    private boolean keepOriginalColumns = false;

    /** 原始列到新列的映射（用于重命名） */
    private final Map<String, String> columnAliasMap = new HashMap<>();

    /** 需要排除的原始列 */
    private final Set<String> excludedColumns = new HashSet<>();

    /**
     * 列选择表达式列表
     */
    private final List<SelectExpression<?>> selectExpressions = new ArrayList<>();

    public JQuickSelectTransformer(JQuickDataSet inputDataSet) {
        super(inputDataSet, new ArrayList<>());
    }

    public JQuickSelectTransformer(JQuickDataSet inputDataSet, List<JQuickFunctionProvider<?, ?>> providers) {
        super(inputDataSet, providers);
    }

    /**
     * 静态工厂方法 - 创建 SELECT 转换器
     */
    public static JQuickSelectTransformer select(JQuickDataSet dataSet) {
        return new JQuickSelectTransformer(dataSet);
    }

    public static JQuickSelectTransformer select(JQuickDataSet dataSet, JQuickFunctionProvider<?, ?>... providers) {
        return new JQuickSelectTransformer(dataSet, Arrays.asList(providers));
    }

    /**
     * 选择指定列（保持原列名）
     */
    public JQuickSelectTransformer column(String columnName) {
        return column(columnName, columnName);
    }

    /**
     * 选择指定列并重命名
     */
    public JQuickSelectTransformer column(String sourceColumn, String alias) {
        selectExpressions.add(new SelectExpression<>(sourceColumn, alias, null));
        return this;
    }

    /**
     * 选择所有列
     */
    public JQuickSelectTransformer allColumns() {
        for (JQuickColumnMeta col : inputDataSet.getColumns()) {
            if (!excludedColumns.contains(col.getName())) {
                String targetName = columnAliasMap.getOrDefault(col.getName(), col.getName());
                column(col.getName(), targetName);
            }
        }
        return this;
    }

    /**
     * 添加固定值列
     */
    public <T> JQuickSelectTransformer constant(String alias, T value, Class<T> type) {
        JQuickConstantProvider<T> provider = new JQuickConstantProvider<>(alias, type, value);
        providers.add(provider);
        return this;
    }

    public JQuickSelectTransformer constantString(String alias, String value) {
        return constant(alias, value, String.class);
    }

    public JQuickSelectTransformer constantInt(String alias, Integer value) {
        return constant(alias, value, Integer.class);
    }

    public JQuickSelectTransformer constantDouble(String alias, Double value) {
        return constant(alias, value, Double.class);
    }

    public JQuickSelectTransformer constantBoolean(String alias, Boolean value) {
        return constant(alias, value, Boolean.class);
    }

    /**
     * 添加表达式列（自定义 Function）
     */
    public <T> JQuickSelectTransformer expression(String alias, Class<T> type, Function<JQuickRow, T> expr) {
        JQuickCompositeProvider<T> provider = new JQuickCompositeProvider<>(alias, type, expr);
        providers.add(provider);
        return this;
    }

    /**
     * 添加表达式列（依赖指定列）
     */
    public <T> JQuickSelectTransformer expression(String alias, Class<T> type, Function<JQuickRow, T> expr, String... dependentColumns) {
        JQuickCompositeProvider<T> provider = new JQuickCompositeProvider<>(alias, type, expr, dependentColumns);
        providers.add(provider);
        return this;
    }

    /**
     * 字符串拼接
     */
    public JQuickSelectTransformer concat(String alias, String delimiter, String... columns) {
        providers.add(JQuickCompositeProvider.concat(alias, delimiter, columns));
        return this;
    }

    /**
     * 数值求和
     */
    public JQuickSelectTransformer sum(String alias, String... numericColumns) {
        providers.add(JQuickCompositeProvider.sum(alias, numericColumns));
        return this;
    }

    /**
     * 创建条件表达式构建器
     */
    public <T> CaseBuilder<T> caseBuilder(String alias, Class<T> type) {
        return new CaseBuilder<>(this, alias, type);
    }

    /**
     * 简单 IF 条件
     */
    public <T> JQuickSelectTransformer ifThen(String alias, Class<T> type, Function<JQuickRow, Boolean> predicate, T trueValue, T falseValue) {
        JQuickConditionalProvider<T> provider = new JQuickConditionalProvider<>(alias, type, falseValue);
        provider.when(predicate, trueValue);
        providers.add(provider);
        return this;
    }

    /**
     * 简单 IF 条件（基于列相等判断）
     */
    public <T> JQuickSelectTransformer ifEquals(String alias, Class<T> type,
                                                String column, Object expected, T trueValue, T falseValue) {
        JQuickConditionalProvider<T> provider = new JQuickConditionalProvider<>(alias, type, falseValue);
        provider.whenEquals(column, expected, trueValue);
        providers.add(provider);
        return this;
    }

    /**
     * 处理 NULL 值，用默认值替换
     */
    public <T> JQuickSelectTransformer coalesce(String alias, Class<T> type, String sourceColumn, T defaultValue) {
        JQuickDefaultValueProvider<T> provider = new JQuickDefaultValueProvider<>(sourceColumn, alias, type, defaultValue);
        providers.add(provider);
        return this;
    }

    /**
     * COALESCE - 返回第一个非 NULL 的值
     */
    @SafeVarargs
    public final <T> JQuickSelectTransformer coalesce(String alias, Class<T> type, JQuickFunctionProvider<JQuickRow, T>... providers) {
        return expression(alias, type, row -> {
            for (JQuickFunctionProvider<JQuickRow, T> p : providers) {
                T val = p.apply(row);
                if (val != null) return val;
            }
            return null;
        });
    }

    /**
     * 类型转换
     */
    public <T> JQuickSelectTransformer cast(String sourceColumn, String alias, Class<T> targetType) {
        JQuickColumnProvider<T> provider = JQuickColumnProvider.of(sourceColumn, alias, targetType);
        providers.add(provider);
        return this;
    }

    public JQuickSelectTransformer asString(String sourceColumn, String alias) {
        return cast(sourceColumn, alias, String.class);
    }

    public JQuickSelectTransformer asInt(String sourceColumn, String alias) {
        return cast(sourceColumn, alias, Integer.class);
    }

    public JQuickSelectTransformer asLong(String sourceColumn, String alias) {
        return cast(sourceColumn, alias, Long.class);
    }

    public JQuickSelectTransformer asDouble(String sourceColumn, String alias) {
        return cast(sourceColumn, alias, Double.class);
    }
    /**
     * 设置是否保留原始列
     */
    public JQuickSelectTransformer keepOriginalColumns(boolean keep) {
        this.keepOriginalColumns = keep;
        if (keep) {
            rebuildColumns();
        }
        return this;
    }

    /**
     * 重命名列（仅在 keepOriginalColumns=true 时生效）
     */
    public JQuickSelectTransformer alias(String originalColumn, String newAlias) {
        this.columnAliasMap.put(originalColumn, newAlias);
        if (keepOriginalColumns) {
            rebuildColumns();
        }
        return this;
    }

    /**
     * 排除指定列（仅在 keepOriginalColumns=true 时生效）
     */
    public JQuickSelectTransformer exclude(String... columns) {
        this.excludedColumns.addAll(Arrays.asList(columns));
        if (keepOriginalColumns) {
            rebuildColumns();
        }
        return this;
    }
    /**
     * 重建列元数据（当 keepOriginalColumns=true 时）
     */
    private void rebuildColumns() {
        transformedColumns.clear();
        // 添加原始列（排除和重命名）
        for (JQuickColumnMeta col : inputDataSet.getColumns()) {
            String colName = col.getName();
            if (!excludedColumns.contains(colName)) {
                String targetName = columnAliasMap.getOrDefault(colName, colName);
                transformedColumns.add(new JQuickColumnMeta(targetName, col.getType(), col.getSource()));
            }
        }
        // 添加表达式列
        for (JQuickFunctionProvider<?, ?> provider : providers) {
            transformedColumns.add(new JQuickColumnMeta(
                    provider.getTargetField(),
                    provider.getTargetClass(),
                    "derived"
            ));
        }
    }

    @Override
    protected void transformRow(JQuickRow sourceRow, JQuickRow targetRow, List<JQuickFunctionProvider<?, ?>> fieldMappings, int rowIndex) {
        if (keepOriginalColumns) {
            targetRow.clear();
            // 保留原始列
            for (Map.Entry<String, Object> entry : sourceRow.entrySet()) {
                String colName = entry.getKey();
                if (!excludedColumns.contains(colName)) {
                    String targetName = columnAliasMap.getOrDefault(colName, colName);
                    targetRow.put(targetName, entry.getValue());
                }
            }
        } else {
            targetRow.clear();
        }
        // 处理表达式列
        for (JQuickFunctionProvider<?, ?> provider : fieldMappings) {
            @SuppressWarnings("unchecked")
            JQuickFunctionProvider<JQuickRow, Object> typedProvider =
                    (JQuickFunctionProvider<JQuickRow, Object>) provider;
            Object value = typedProvider.apply(sourceRow);
            targetRow.put(provider.getTargetField(), value);
        }
    }

    @Override
    protected void preProcess() {
        // 处理列选择表达式
        for (SelectExpression<?> expr : selectExpressions) {
            if (expr.provider == null) {
                JQuickColumnProvider<Object> provider = new JQuickColumnProvider<>(
                        expr.sourceColumn, expr.alias, Object.class);
                providers.add(provider);
            }
        }
        selectExpressions.clear();

        if (!keepOriginalColumns) {
            for (JQuickRow row : transformedRows) {
                row.clear();
            }
        } else {
            rebuildColumns();
        }
    }

    /**
     * 列选择表达式
     */
    private static class SelectExpression<T> {
        final String sourceColumn;
        final String alias;
        final JQuickFunctionProvider<JQuickRow, T> provider;

        SelectExpression(String sourceColumn, String alias, JQuickFunctionProvider<JQuickRow, T> provider) {
            this.sourceColumn = sourceColumn;
            this.alias = alias;
            this.provider = provider;
        }
    }

    /**
     * CASE WHEN 构建器
     */
    public static class CaseBuilder<T> {
        private final JQuickSelectTransformer transformer;
        private final String alias;
        private final Class<T> type;
        private final List<Condition<T>> conditions = new ArrayList<>();
        private T defaultValue;

        private CaseBuilder(JQuickSelectTransformer transformer, String alias, Class<T> type) {
            this.transformer = transformer;
            this.alias = alias;
            this.type = type;
        }

        public CaseBuilder<T> when(Function<JQuickRow, Boolean> predicate, T value) {
            conditions.add(new Condition<>(predicate, value));
            return this;
        }

        public CaseBuilder<T> whenEquals(String column, Object expected, T value) {
            return when(row -> Objects.equals(row.get(column), expected), value);
        }

        public CaseBuilder<T> otherwise(T defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        public JQuickSelectTransformer end() {
            JQuickConditionalProvider<T> provider = new JQuickConditionalProvider<>(alias, type, defaultValue);
            for (Condition<T> cond : conditions) {
                provider.when(cond.predicate, cond.value);
            }
            transformer.providers.add(provider);
            return transformer;
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
}



