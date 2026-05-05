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

public class JQuickSparkCountGroupByProvider extends JQuickSparkGroupByAggregationProvider<Long> {

    private final boolean distinct;

    private String countColumn;

    public JQuickSparkCountGroupByProvider(List<String> groupByColumns, String resultColumnName, SparkSession spark) {
        this(groupByColumns, resultColumnName, null, false, spark);
    }

    public JQuickSparkCountGroupByProvider(List<String> groupByColumns, String resultColumnName, String countColumn, boolean distinct, SparkSession spark) {
        super(groupByColumns, resultColumnName, spark);
        this.countColumn = countColumn;
        this.distinct = distinct;
    }

    @Override
    protected Dataset<Row> doAggregate(Dataset<Row> df) {
        Column[] groupCols = groupByColumns.stream()
                .map(functions::col)
                .toArray(Column[]::new);
        if (countColumn == null) {
            // 计数所有行
            return df.groupBy(groupCols).agg(count(col("*")).alias(resultColumnName));
        } else if (distinct) {
            // 去重计数
            return df.groupBy(groupCols).agg(countDistinct(col(countColumn)).alias(resultColumnName));
        } else {
            // 非空计数
            return df.groupBy(groupCols)
                    .agg(count(col(countColumn)).alias(resultColumnName));
        }
    }
    @Override
    public JQuickComputeTypeImpl getType() {
        return new JQuickSparkComputeTypeCountImpl();
    }
    private static class JQuickSparkComputeTypeCountImpl extends JQuickSparkComputeTypeImpl {
        @Override
        public String getMethod() {
            return JQuickProviderMethodConstants.COUNT;
        }
    }
}
