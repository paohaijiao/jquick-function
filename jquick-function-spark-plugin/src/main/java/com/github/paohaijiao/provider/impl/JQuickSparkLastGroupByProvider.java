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

import com.github.paohaijiao.compute.JQuickComputeTypeImpl;
import com.github.paohaijiao.compute.JQuickSparkComputeTypeImpl;
import com.github.paohaijiao.core.constant.JQuickProviderMethodConstants;
import com.github.paohaijiao.provider.JQuickSparkGroupByAggregationProvider;
import org.apache.spark.sql.*;

import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.last;

/**
 * Spark分布式最后一个值聚合器
 * 使用 last 函数
 */
public class JQuickSparkLastGroupByProvider extends JQuickSparkGroupByAggregationProvider<Object> {

    private final String lastColumn;
    private final boolean ignoreNulls; // 是否忽略 null 值

    public JQuickSparkLastGroupByProvider(List<String> groupByColumns, String resultColumnName, String lastColumn, SparkSession spark) {
        this(groupByColumns, resultColumnName, lastColumn, false, spark);
    }

    public JQuickSparkLastGroupByProvider(List<String> groupByColumns, String resultColumnName, String lastColumn, boolean ignoreNulls, SparkSession spark) {
        super(groupByColumns, resultColumnName, spark);
        this.lastColumn = lastColumn;
        this.ignoreNulls = ignoreNulls;
    }

    @Override
    protected Dataset<Row> doAggregate(Dataset<Row> df) {
        Column[] groupCols = groupByColumns.stream().map(functions::col).toArray(Column[]::new);
        return df.groupBy(groupCols).agg(last(col(lastColumn), ignoreNulls).alias(resultColumnName));
    }

    @Override
    public JQuickComputeTypeImpl getType() {
        return new JQuickSparkComputeTypeLastImpl();
    }

    private static class JQuickSparkComputeTypeLastImpl extends JQuickSparkComputeTypeImpl {
        @Override
        public String getMethod() {
            return JQuickProviderMethodConstants.LAST;
        }
    }
}
