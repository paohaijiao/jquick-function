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
import com.github.paohaijiao.compute.JQuickFlinkComputeTypeImpl;
import com.github.paohaijiao.core.constant.JQuickProviderMethodConstants;
import com.github.paohaijiao.provider.JQuickFlinkGroupByAggregationProvider;
import com.github.paohaijiao.statement.JQuickRow;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.List;

/**
 * Flink 分布式最小值聚合器
 */
public class JQuickFlinkMinGroupByProvider extends JQuickFlinkGroupByAggregationProvider<Object> {

    private final String minColumn;

    public JQuickFlinkMinGroupByProvider(List<String> groupByColumns, String resultColumnName, String minColumn, ExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        super(groupByColumns, resultColumnName, env, tableEnv);
        this.minColumn = minColumn;
    }

    @Override
    protected DataSet<JQuickRow> doAggregate(DataSet<JQuickRow> dataSet) throws Exception {
        DataSet<Tuple2<String, Object>> mapped = dataSet.map(new MapFunction<JQuickRow, Tuple2<String, Object>>() {
            @Override
            public Tuple2<String, Object> map(JQuickRow row) throws Exception {
                String key = createGroupKey(row);
                Object value = row.get(minColumn);
                return new Tuple2<>(key, value);
            }
        });

        DataSet<Tuple2<String, Object>> reduced = mapped
                .groupBy(0)
                .reduce(new ReduceFunction<Tuple2<String, Object>>() {
                    @Override
                    public Tuple2<String, Object> reduce(Tuple2<String, Object> t1,
                                                         Tuple2<String, Object> t2) throws Exception {
                        Object v1 = t1.f1;
                        Object v2 = t2.f1;

                        if (v1 == null) return t2;
                        if (v2 == null) return t1;

                        if (v1 instanceof Comparable && v2 instanceof Comparable) {
                            int cmp = ((Comparable) v1).compareTo(v2);
                            return cmp <= 0 ? t1 : t2;
                        }
                        return t1;
                    }
                });

        return reduced.map(new MapFunction<Tuple2<String, Object>, JQuickRow>() {
            @Override
            public JQuickRow map(Tuple2<String, Object> tuple) throws Exception {
                JQuickRow result = parseGroupKey(tuple.f0, null);
                result.put(resultColumnName, tuple.f1);
                return result;
            }
        });
    }

    @Override
    protected Class<?> getResultType() {
        return Object.class;
    }

    @Override
    public JQuickComputeTypeImpl getType() {
        return new JQuickFlinkComputeTypeMinImpl();
    }

    private static class JQuickFlinkComputeTypeMinImpl extends JQuickFlinkComputeTypeImpl {
        @Override
        public String getMethod() {
            return JQuickProviderMethodConstants.MIN;
        }
    }
}
