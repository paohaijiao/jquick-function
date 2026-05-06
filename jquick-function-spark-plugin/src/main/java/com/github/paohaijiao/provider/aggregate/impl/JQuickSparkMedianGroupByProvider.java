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
import com.github.paohaijiao.core.constant.JQuickProviderMethodConstants;
import com.github.paohaijiao.provider.aggregate.JQuickSparkGroupByAggregationProvider;
import org.apache.spark.sql.*;

import java.util.List;

import static org.apache.spark.sql.functions.expr;

/**
 * Spark分布式中位数聚合器
 * 使用 approxQuantile 或 percentile_approx 函数
 */
public class JQuickSparkMedianGroupByProvider extends JQuickSparkGroupByAggregationProvider<Double> {

    private final String medianColumn;

    private final double probability;

    private final double relativeError;

    public JQuickSparkMedianGroupByProvider(List<String> groupByColumns, String resultColumnName, String medianColumn, SparkSession spark) {
        this(groupByColumns, resultColumnName, medianColumn, 0.5, 0.0, spark);
    }

    public JQuickSparkMedianGroupByProvider(List<String> groupByColumns, String resultColumnName, String medianColumn, double probability, double relativeError, SparkSession spark) {
        super(groupByColumns, resultColumnName, spark);
        this.medianColumn = medianColumn;
        this.probability = probability;
        this.relativeError = relativeError;
    }

    @Override
    protected Dataset<Row> doAggregate(Dataset<Row> df) {
        Column[] groupCols = groupByColumns.stream()
                .map(functions::col)
                .toArray(Column[]::new);
        // 使用 percentile_approx 计算中位数
        return df.groupBy(groupCols).agg(expr("percentile_approx(" + medianColumn + ", " + probability + ", " + relativeError + ")").alias(resultColumnName));
    }

    @Override
    public JQuickComputeTypeImpl getType() {
        return new JQuickSparkComputeTypeMedianImpl();
    }

    private static class JQuickSparkComputeTypeMedianImpl extends JQuickSparkComputeTypeImpl {
        @Override
        public String getMethod() {
            return JQuickProviderMethodConstants.MEDIAN;
        }
    }
}
