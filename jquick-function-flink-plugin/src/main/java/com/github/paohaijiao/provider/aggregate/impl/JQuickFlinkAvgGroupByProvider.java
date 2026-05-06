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
import com.github.paohaijiao.compute.JQuickFlinkComputeTypeImpl;
import com.github.paohaijiao.core.constant.JQuickProviderMethodConstants;
import com.github.paohaijiao.provider.aggregate.JQuickFlinkGroupByAggregationProvider;
import com.github.paohaijiao.statement.JQuickRow;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.List;

/**
 * Flink 分布式平均值聚合器
 */
public class JQuickFlinkAvgGroupByProvider extends JQuickFlinkGroupByAggregationProvider<Double> {

    private final String avgColumn;

    public JQuickFlinkAvgGroupByProvider(List<String> groupByColumns, String resultColumnName, String avgColumn, ExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        super(groupByColumns, resultColumnName, env, tableEnv);
        this.avgColumn = avgColumn;
    }

    @Override
    protected DataSet<JQuickRow> doAggregate(DataSet<JQuickRow> dataSet) throws Exception {
        // Tuple3<分组键, 和, 计数>
        DataSet<Tuple3<String, Double, Integer>> mapped = dataSet.map(new MapFunction<JQuickRow, Tuple3<String, Double, Integer>>() {
            @Override
            public Tuple3<String, Double, Integer> map(JQuickRow row) throws Exception {
                String key = createGroupKey(row);
                Number value = row.getAs(avgColumn, Number.class);
                double num = value != null ? value.doubleValue() : 0.0;
                return new Tuple3<>(key, num, 1);
            }
        });

        // 分组求和、计数
        DataSet<Tuple3<String, Double, Integer>> reduced = mapped
                .groupBy(0)
                .reduce(new ReduceFunction<Tuple3<String, Double, Integer>>() {
                    @Override
                    public Tuple3<String, Double, Integer> reduce(Tuple3<String, Double, Integer> t1,
                                                                  Tuple3<String, Double, Integer> t2) throws Exception {
                        return new Tuple3<>(t1.f0, t1.f1 + t2.f1, t1.f2 + t2.f2);
                    }
                });

        // 计算平均值并转换回 JQuickRow
        return reduced.map(new MapFunction<Tuple3<String, Double, Integer>, JQuickRow>() {
            @Override
            public JQuickRow map(Tuple3<String, Double, Integer> tuple) throws Exception {
                JQuickRow result = parseGroupKey(tuple.f0, null);
                double avg = tuple.f2 > 0 ? tuple.f1 / tuple.f2 : 0.0;
                result.put(resultColumnName, avg);
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
        return new JQuickFlinkComputeTypeAvgImpl();
    }

    private static class JQuickFlinkComputeTypeAvgImpl extends JQuickFlinkComputeTypeImpl {
        @Override
        public String getMethod() {
            return JQuickProviderMethodConstants.AVG;
        }
    }
}