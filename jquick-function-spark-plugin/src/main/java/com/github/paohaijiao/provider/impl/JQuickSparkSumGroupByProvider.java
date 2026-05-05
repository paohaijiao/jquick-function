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

import static org.apache.spark.sql.functions.*;

/**
 * Spark分布式求和聚合器
 */
public class JQuickSparkSumGroupByProvider extends JQuickSparkGroupByAggregationProvider<Double> {

    private final String sumColumn;

    public JQuickSparkSumGroupByProvider(List<String> groupByColumns, String resultColumnName, String sumColumn, SparkSession spark) {
        super(groupByColumns, resultColumnName, spark);
        this.sumColumn = sumColumn;
    }

    @Override
    protected Dataset<Row> doAggregate(Dataset<Row> df) {
        Column[] groupCols = groupByColumns.stream()
                .map(functions::col)
                .toArray(Column[]::new);
        return df.groupBy(groupCols).agg(sum(col(sumColumn)).alias(resultColumnName));
    }

    @Override
    public JQuickComputeTypeImpl getType() {
        return new JQuickSparkComputeTypeSumImpl();
    }
    private static class JQuickSparkComputeTypeSumImpl extends JQuickSparkComputeTypeImpl {
        @Override
        public String getMethod() {
            return JQuickProviderMethodConstants.SUM;
        }
    }
}