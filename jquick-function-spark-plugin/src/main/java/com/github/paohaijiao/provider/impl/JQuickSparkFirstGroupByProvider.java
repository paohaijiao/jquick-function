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
package com.github.paohaijiao.provider.impl;

/**
 * packageName com.github.paohaijiao.provider.impl
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/5
 */

import com.github.paohaijiao.compute.JQuickComputeTypeImpl;
import com.github.paohaijiao.compute.JQuickSparkComputeTypeImpl;
import com.github.paohaijiao.core.constant.JQuickProviderMethodConstants;
import com.github.paohaijiao.provider.JQuickSparkGroupByAggregationProvider;
import org.apache.spark.sql.*;

import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.first;

/**
 * Spark分布式第一个值聚合器
 * 使用 first 函数
 */
public class JQuickSparkFirstGroupByProvider extends JQuickSparkGroupByAggregationProvider<Object> {

    private final String firstColumn;

    private final boolean ignoreNulls; // 是否忽略 null 值

    public JQuickSparkFirstGroupByProvider(List<String> groupByColumns, String resultColumnName, String firstColumn, SparkSession spark) {
        this(groupByColumns, resultColumnName, firstColumn, false, spark);
    }

    public JQuickSparkFirstGroupByProvider(List<String> groupByColumns, String resultColumnName, String firstColumn, boolean ignoreNulls, SparkSession spark) {
        super(groupByColumns, resultColumnName, spark);
        this.firstColumn = firstColumn;
        this.ignoreNulls = ignoreNulls;
    }

    @Override
    protected Dataset<Row> doAggregate(Dataset<Row> df) {
        Column[] groupCols = groupByColumns.stream()
                .map(functions::col)
                .toArray(Column[]::new);
        // Spark 的 first 函数默认忽略 null，可以设置第二参数
        return df.groupBy(groupCols).agg(first(col(firstColumn), ignoreNulls).alias(resultColumnName));
    }

    @Override
    public JQuickComputeTypeImpl getType() {
        return new JQuickSparkComputeTypeFirstImpl();
    }

    private static class JQuickSparkComputeTypeFirstImpl extends JQuickSparkComputeTypeImpl {
        @Override
        public String getMethod() {
            return JQuickProviderMethodConstants.FIRST;
        }
    }
}
