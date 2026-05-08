package com.github.paohaijiao.transform;
import com.github.paohaijiao.provider.JQuickFunctionProvider;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;
import com.github.paohaijiao.statement.JQuickColumnMeta;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * SELECT 转换器 - 从源数据集中选择并转换指定的列
 * 类似于 SQL 的 SELECT 语句，可以：
 * - 选择指定列
 * - 重命名列
 * - 类型转换
 * - 计算新列
 * - 应用表达式
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

    public JQuickSelectTransformer(JQuickDataSet inputDataSet, List<JQuickFunctionProvider<?, ?>> providers) {
        super(inputDataSet, providers);
    }

    /**
     * 静态工厂方法 - 创建 SELECT 转换器
     */
    public static JQuickSelectTransformer select(JQuickDataSet dataSet, JQuickFunctionProvider<?, ?>... providers) {
        return new JQuickSelectTransformer(dataSet, Arrays.asList(providers));
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
     * 重命名列
     */
    public JQuickSelectTransformer alias(String originalColumn, String newAlias) {
        this.columnAliasMap.put(originalColumn, newAlias);
        if (keepOriginalColumns) {
            rebuildColumns();
        }
        return this;
    }

    /**
     * 排除指定列
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
        for (JQuickColumnMeta col : inputDataSet.getColumns()) {
            String colName = col.getName();
            if (!excludedColumns.contains(colName)) {
                String targetName = columnAliasMap.getOrDefault(colName, colName);
                transformedColumns.add(new JQuickColumnMeta(targetName, col.getType(), col.getSource()));
            }
        }
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
        for (JQuickFunctionProvider<?, ?> provider : fieldMappings) {
            @SuppressWarnings("unchecked")
            JQuickFunctionProvider<JQuickRow, Object> typedProvider = (JQuickFunctionProvider<JQuickRow, Object>) provider;
            Object value = typedProvider.apply(sourceRow);
            targetRow.put(provider.getTargetField(), value);
        }
    }

    @Override
    protected void preProcess() {
        if (!keepOriginalColumns) {// 不保留原始列时，清空所有行数据
            for (JQuickRow row : transformedRows) {
                row.clear();
            }
        }
    }
}






