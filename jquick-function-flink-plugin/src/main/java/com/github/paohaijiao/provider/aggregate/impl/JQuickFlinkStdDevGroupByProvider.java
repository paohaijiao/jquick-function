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
import com.github.paohaijiao.core.constant.JQuickAggregateProviderMethodConstants;
import com.github.paohaijiao.provider.aggregate.JQuickFlinkGroupByAggregationProvider;
import com.github.paohaijiao.statement.JQuickRow;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * Flink 分布式标准差聚合器
 */
public class JQuickFlinkStdDevGroupByProvider extends JQuickFlinkGroupByAggregationProvider<Double> {

    private final String stddevColumn;
    private final boolean isSample;

    public JQuickFlinkStdDevGroupByProvider(List<String> groupByColumns, String resultColumnName, String stddevColumn, ExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        this(groupByColumns, resultColumnName, stddevColumn, false, env, tableEnv);
    }

    public JQuickFlinkStdDevGroupByProvider(List<String> groupByColumns, String resultColumnName, String stddevColumn, boolean isSample, ExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        super(groupByColumns, resultColumnName, env, tableEnv);
        this.stddevColumn = stddevColumn;
        this.isSample = isSample;
    }

    @Override
    protected DataSet<JQuickRow> doAggregate(DataSet<JQuickRow> dataSet) throws Exception {
        // 使用 Tuple4: <分组键, 和, 平方和, 计数>
        DataSet<Tuple4<String, Double, Double, Integer>> mapped = dataSet.map(
                new MapFunction<JQuickRow, Tuple4<String, Double, Double, Integer>>() {
                    @Override
                    public Tuple4<String, Double, Double, Integer> map(JQuickRow row) throws Exception {
                        String key = createGroupKey(row);
                        Number value = row.getAs(stddevColumn, Number.class);
                        double num = value != null ? value.doubleValue() : 0.0;
                        return new Tuple4<>(key, num, num * num, 1);
                    }
                });

        // 分组聚合
        DataSet<Tuple4<String, Double, Double, Integer>> reduced = mapped
                .groupBy(0)
                .reduceGroup(new GroupReduceFunction<Tuple4<String, Double, Double, Integer>,
                        Tuple4<String, Double, Double, Integer>>() {
                    @Override
                    public void reduce(Iterable<Tuple4<String, Double, Double, Integer>> values,
                                       Collector<Tuple4<String, Double, Double, Integer>> out) throws Exception {
                        String key = null;
                        double sum = 0.0;
                        double sumSq = 0.0;
                        int count = 0;

                        for (Tuple4<String, Double, Double, Integer> val : values) {
                            key = val.f0;
                            sum += val.f1;
                            sumSq += val.f2;
                            count += val.f3;
                        }

                        out.collect(new Tuple4<>(key, sum, sumSq, count));
                    }
                });

        // 计算标准差
        return reduced.map(new MapFunction<Tuple4<String, Double, Double, Integer>, JQuickRow>() {
            @Override
            public JQuickRow map(Tuple4<String, Double, Double, Integer> tuple) throws Exception {
                JQuickRow result = parseGroupKey(tuple.f0, null);
                double sum = tuple.f1;
                double sumSq = tuple.f2;
                int count = tuple.f3;

                double stddev = 0.0;
                if (count > 0) {
                    double mean = sum / count;
                    double variance;
                    if (isSample && count > 1) {
                        variance = (sumSq - sum * sum / count) / (count - 1);
                    } else {
                        variance = (sumSq - sum * sum / count) / count;
                    }
                    stddev = Math.sqrt(variance);
                }

                result.put(resultColumnName, stddev);
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
        return new JQuickFlinkComputeTypeStdImpl();
    }
    private static class JQuickFlinkComputeTypeStdImpl extends JQuickFlinkComputeTypeImpl {
        @Override
        public String getMethod() {
            return JQuickAggregateProviderMethodConstants.STDDEV;
        }
    }
}
