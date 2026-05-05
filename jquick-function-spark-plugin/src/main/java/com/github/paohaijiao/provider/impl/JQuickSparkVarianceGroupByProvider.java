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

import static org.apache.spark.sql.functions.*;

/**
 * Spark分布式方差聚合器
 */
public class JQuickSparkVarianceGroupByProvider extends JQuickSparkGroupByAggregationProvider<Double> {

    private final String varianceColumn;

    private final boolean isSample; // true: 样本方差(var_samp), false: 总体方差(var_pop)

    public JQuickSparkVarianceGroupByProvider(List<String> groupByColumns, String resultColumnName, String varianceColumn, SparkSession spark) {
        this(groupByColumns, resultColumnName, varianceColumn, false, spark);
    }

    public JQuickSparkVarianceGroupByProvider(List<String> groupByColumns, String resultColumnName, String varianceColumn, boolean isSample, SparkSession spark) {
        super(groupByColumns, resultColumnName, spark);
        this.varianceColumn = varianceColumn;
        this.isSample = isSample;
    }

    @Override
    protected Dataset<Row> doAggregate(Dataset<Row> df) {
        Column[] groupCols = groupByColumns.stream()
                .map(functions::col)
                .toArray(Column[]::new);

        if (isSample) {
            return df.groupBy(groupCols).agg(var_samp(col(varianceColumn)).alias(resultColumnName));
        } else {
            return df.groupBy(groupCols).agg(var_pop(col(varianceColumn)).alias(resultColumnName));
        }
    }

    @Override
    public JQuickComputeTypeImpl getType() {
        return new JQuickSparkComputeTypeVarianceImpl();
    }

    private static class JQuickSparkComputeTypeVarianceImpl extends JQuickSparkComputeTypeImpl {
        @Override
        public String getMethod() {
            return JQuickProviderMethodConstants.VARIANCE;
        }
    }
}
