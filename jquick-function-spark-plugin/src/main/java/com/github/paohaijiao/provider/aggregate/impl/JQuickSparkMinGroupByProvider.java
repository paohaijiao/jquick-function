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
package com.github.paohaijiao.provider.aggregate.impl;

import com.github.paohaijiao.compute.JQuickComputeTypeImpl;
import com.github.paohaijiao.compute.JQuickSparkComputeTypeImpl;
import com.github.paohaijiao.core.constant.JQuickAggregateProviderMethodConstants;
import com.github.paohaijiao.provider.aggregate.JQuickSparkGroupByAggregationProvider;
import org.apache.spark.sql.*;

import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.min;

/**
 * Spark分布式最小值聚合器
 */
public class JQuickSparkMinGroupByProvider extends JQuickSparkGroupByAggregationProvider<Object> {

    private final String minColumn;

    public JQuickSparkMinGroupByProvider(List<String> groupByColumns, String resultColumnName, String minColumn, SparkSession spark) {
        super(groupByColumns, resultColumnName, spark);
        this.minColumn = minColumn;
    }

    @Override
    protected Dataset<Row> doAggregate(Dataset<Row> df) {
        Column[] groupCols = groupByColumns.stream()
                .map(functions::col)
                .toArray(Column[]::new);
        return df.groupBy(groupCols).agg(min(col(minColumn)).alias(resultColumnName));
    }

    @Override
    public JQuickComputeTypeImpl getType() {
        return new JQuickSparkComputeTypeMinImpl();
    }

    private static class JQuickSparkComputeTypeMinImpl extends JQuickSparkComputeTypeImpl {
        @Override
        public String getMethod() {
            return JQuickAggregateProviderMethodConstants.MIN;
        }
    }
}
