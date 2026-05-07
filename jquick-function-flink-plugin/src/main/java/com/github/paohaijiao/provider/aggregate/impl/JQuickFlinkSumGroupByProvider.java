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

/**
 * packageName com.github.paohaijiao.provider
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/5
 */

import com.github.paohaijiao.compute.JQuickComputeTypeImpl;
import com.github.paohaijiao.compute.JQuickFlinkComputeTypeImpl;
import com.github.paohaijiao.core.constant.JQuickAggregateProviderMethodConstants;
import com.github.paohaijiao.provider.aggregate.JQuickFlinkGroupByAggregationProvider;
import com.github.paohaijiao.statement.JQuickRow;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.List;

/**
 * Flink 分布式求和聚合器
 * 按分组字段对指定列进行求和
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/1/9
 */
public class JQuickFlinkSumGroupByProvider extends JQuickFlinkGroupByAggregationProvider<Double> {

    private final String sumColumn;

    public JQuickFlinkSumGroupByProvider(List<String> groupByColumns, String resultColumnName, String sumColumn, ExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        super(groupByColumns, resultColumnName, env, tableEnv);
        this.sumColumn = sumColumn;
    }

    @Override
    protected DataSet<JQuickRow> doAggregate(DataSet<JQuickRow> dataSet) throws Exception {
        //将JQuickRow 转换为 Tuple2<分组键, 求和值>
        DataSet<Tuple2<String, Double>> mapped = dataSet.map(new MapFunction<JQuickRow, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(JQuickRow row) throws Exception {
                String key = createGroupKey(row);
                Number value = row.getAs(sumColumn, Number.class);
                double num = value != null ? value.doubleValue() : 0.0;
                return new Tuple2<>(key, num);
            }
        });
        //按分组键进行求和聚合
        DataSet<Tuple2<String, Double>> reduced = mapped
                .groupBy(0)
                .reduce(new ReduceFunction<Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> reduce(Tuple2<String, Double> t1, Tuple2<String, Double> t2) throws Exception {
                        return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
                    }
                });

        // 将Tuple2 转换回 JQuickRow
        return reduced.map(new MapFunction<Tuple2<String, Double>, JQuickRow>() {
            @Override
            public JQuickRow map(Tuple2<String, Double> tuple) throws Exception {
                JQuickRow result = parseGroupKey(tuple.f0, null);
                result.put(resultColumnName, tuple.f1);
                return result;
            }
        });
    }

    @Override
    protected Class<?> getResultType() {
        return Double.class;
    }

    @Override
    public JQuickComputeTypeImpl getType() {
        return new JQuickFlinkComputeTypeSumImpl();
    }

    private static class JQuickFlinkComputeTypeSumImpl extends JQuickFlinkComputeTypeImpl {
        @Override
        public String getMethod() {
            return JQuickAggregateProviderMethodConstants.SUM;
        }
    }
}