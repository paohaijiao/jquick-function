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

import static org.apache.spark.sql.functions.*;

/**
 * Spark分布式标准差聚合器
 */
public class JQuickSparkStdDevGroupByProvider extends JQuickSparkGroupByAggregationProvider<Double> {

    private final String stddevColumn;

    private final boolean isSample; // true: 样本标准差(stddev_samp), false: 总体标准差(stddev_pop)

    public JQuickSparkStdDevGroupByProvider(List<String> groupByColumns, String resultColumnName, String stddevColumn, SparkSession spark) {
        this(groupByColumns, resultColumnName, stddevColumn, false, spark);
    }

    public JQuickSparkStdDevGroupByProvider(List<String> groupByColumns, String resultColumnName, String stddevColumn, boolean isSample, SparkSession spark) {
        super(groupByColumns, resultColumnName, spark);
        this.stddevColumn = stddevColumn;
        this.isSample = isSample;
    }

    @Override
    protected Dataset<Row> doAggregate(Dataset<Row> df) {
        Column[] groupCols = groupByColumns.stream()
                .map(functions::col)
                .toArray(Column[]::new);

        if (isSample) {
            return df.groupBy(groupCols).agg(stddev_samp(col(stddevColumn)).alias(resultColumnName));
        } else {
            return df.groupBy(groupCols).agg(stddev_pop(col(stddevColumn)).alias(resultColumnName));
        }
    }

    @Override
    public JQuickComputeTypeImpl getType() {
        return new JQuickSparkComputeTypeStdDevImpl();
    }

    private static class JQuickSparkComputeTypeStdDevImpl extends JQuickSparkComputeTypeImpl {
        @Override
        public String getMethod() {
            return JQuickAggregateProviderMethodConstants.STDDEV;
        }
    }
}
