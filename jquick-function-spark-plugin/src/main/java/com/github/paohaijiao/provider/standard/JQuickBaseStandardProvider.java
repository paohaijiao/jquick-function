/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright (c) [2025-2099] Martin (goudingcheng@gmail.com)
 */
package com.github.paohaijiao.provider.standard;

import com.github.paohaijiao.provider.JStandardProvider;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.udf;

/**
 * 抽象标准转换器基类 - 自动集成 Spark UDF，支持 List 参数
 *
 * <p>子类只需实现 {@link #transform(List)} 方法即可
 * <p>自动支持本地模式和 Spark 模式
 * <p>Spark 模式下，多个字段会被包装为 List 传入
 *
 * @param <R> 输出类型
 */
public abstract class JQuickBaseStandardProvider<R> implements JStandardProvider<Object, R>, Serializable {

    private static final long serialVersionUID = 1L;

    /** 依赖的源字段列表 */
    protected final List<String> dependentColumns;

    /** 输出的新字段名称 */
    protected final String outputColumnName;

    /** Spark UDF 实例（懒加载） */
    private transient volatile UserDefinedFunction udf;

    public JQuickBaseStandardProvider(List<String> dependentColumns, String outputColumnName) {
        if (dependentColumns == null || dependentColumns.isEmpty()) {
            throw new IllegalArgumentException("dependentColumns cannot be null or empty");
        }
        this.dependentColumns = new ArrayList<>(dependentColumns);
        this.outputColumnName = outputColumnName;
    }

    public JQuickBaseStandardProvider(String dependentColumn, String outputColumnName) {
        this(Collections.singletonList(dependentColumn), outputColumnName);
    }

    @Override
    public R apply(List<Object> values) {
        if (values == null || values.isEmpty()) {
            return onEmptyValues();
        }
        return transform(values);
    }

    /**
     * 核心转换方法 - 子类必须实现
     *
     * @param values 依赖字段的值列表，顺序与 dependentColumns 一致
     * @return 转换后的值
     */
    protected abstract R transform(List<Object> values);

    /**
     * 当输入值为 null 或空时的处理策略（子类可覆盖）
     */
    protected R onEmptyValues() {
        return null;
    }

    @Override
    public List<String> getDependentColumns() {
        return dependentColumns;
    }

    @Override
    public String getOutputColumnName() {
        return outputColumnName;
    }

    @Override
    public boolean skipOnNull() {
        return true;
    }
    /**
     * 获取 Spark UDF 实例（自动创建并缓存）
     */
    public UserDefinedFunction getUDF() {
        if (udf == null) {
            synchronized (this) {
                if (udf == null) {
                    udf = createUDF();
                }
            }
        }
        return udf;
    }

    /**
     * 创建 Spark UDF
     * 使用 UDF1 接收 Seq[Object] 类型，转换为 List 后处理
     */
    @SuppressWarnings("unchecked")
    protected UserDefinedFunction createUDF() {
        // 使用 UDF1 接收 scala.collection.Seq 类型
        return udf(new org.apache.spark.sql.api.java.UDF1<scala.collection.Seq<Object>, R>() {
            @Override
            public R call(scala.collection.Seq<Object> seq) throws Exception {
                // 将 Scala Seq 转换为 Java List
                List<Object> values = scala.collection.JavaConverters.seqAsJavaList(seq);
                return apply(values);
            }
        }, getSparkDataType());
    }

    /**
     * 将当前转换器应用到 DataFrame
     *
     * @param df 原始 DataFrame
     * @return 添加新列后的 DataFrame
     */
    public Dataset<Row> transform(Dataset<Row> df) {
        if (dependentColumns.isEmpty()) {
            throw new IllegalStateException("No dependent columns specified");
        }
        // 构建 Column 数组
        Column[] columns = dependentColumns.stream()
                .map(columnName -> functions.col(columnName))
                .toArray(Column[]::new);
        // 使用 udf 的 apply 方法
        return df.withColumn(outputColumnName, getUDF().apply(columns));
    }

    /**
     * 将当前转换器应用到 DataFrame，并控制是否保留原始列
     *
     * @param df 原始 DataFrame
     * @param keepOriginalColumns 是否保留原始依赖列
     * @return 转换后的 DataFrame
     */
    public Dataset<Row> transform(Dataset<Row> df, boolean keepOriginalColumns) {
        Dataset<Row> result = transform(df);
        if (!keepOriginalColumns) {
            for (String col : dependentColumns) {
                if (!col.equals(outputColumnName)) {
                    result = result.drop(col);
                }
            }
        }
        return result;
    }

    /**
     * 将当前转换器应用到 DataFrame（使用列索引）
     *
     * @param df 原始 DataFrame
     * @param columnIndices 依赖列的索引
     * @return 添加新列后的 DataFrame
     */
    public Dataset<Row> transformByIndex(Dataset<Row> df, int... columnIndices) {
        Column[] columns = new Column[columnIndices.length];
        for (int i = 0; i < columnIndices.length; i++) {
            columns[i] = functions.col(df.columns()[columnIndices[i]]);
        }
        return df.withColumn(outputColumnName, getUDF().apply(columns));
    }

    /**
     * 注册到 Spark Session 供 SQL 使用
     *
     * @param spark SparkSession
     * @param udfName UDF 注册名称
     */
    public void registerUDF(SparkSession spark, String udfName) {
        spark.udf().register(udfName, new org.apache.spark.sql.api.java.UDF1<scala.collection.Seq<Object>, R>() {
            @Override
            public R call(scala.collection.Seq<Object> seq) throws Exception {
                List<Object> values = scala.collection.JavaConverters.seqAsJavaList(seq);
                return apply(values);
            }
        }, getSparkDataType());
    }

    /**
     * 批量应用到 DataFrame（多个 Provider 链式调用）
     *
     * @param df 原始 DataFrame
     * @param providers Provider 列表
     * @return 转换后的 DataFrame
     */
    @SafeVarargs
    public static Dataset<Row> transformChain(Dataset<Row> df, JQuickBaseStandardProvider<?>... providers) {
        Dataset<Row> result = df;
        for (JQuickBaseStandardProvider<?> provider : providers) {
            result = provider.transform(result);
        }
        return result;
    }

    /**
     * 获取 Spark SQL 数据类型（子类必须实现）
     */
    public abstract DataType getSparkDataType();

    /**
     * 获取输出类型的 Class（子类必须实现）
     */
    public abstract Class<R> getOutputClass();
}