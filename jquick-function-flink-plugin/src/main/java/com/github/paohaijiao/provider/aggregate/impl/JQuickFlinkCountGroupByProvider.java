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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.List;

/**
 * Flink 分布式计数聚合器
 */
public class JQuickFlinkCountGroupByProvider extends JQuickFlinkGroupByAggregationProvider<Long> {

    public JQuickFlinkCountGroupByProvider(List<String> groupByColumns, String resultColumnName, ExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        super(groupByColumns, resultColumnName, env, tableEnv);
    }

    @Override
    protected DataSet<JQuickRow> doAggregate(DataSet<JQuickRow> dataSet) throws Exception {
        // 转换为 Tuple2<分组键, 1>
        DataSet<Tuple2<String, Integer>> mapped = dataSet.map(new MapFunction<JQuickRow, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(JQuickRow row) throws Exception {
                String key = createGroupKey(row);
                return new Tuple2<>(key, 1);
            }
        });

        // 分组求和计数
        DataSet<Tuple2<String, Integer>> reduced = mapped
                .groupBy(0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1,
                                                          Tuple2<String, Integer> t2) throws Exception {
                        return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
                    }
                });

        // 转换回 JQuickRow
        return reduced.map(new MapFunction<Tuple2<String, Integer>, JQuickRow>() {
            @Override
            public JQuickRow map(Tuple2<String, Integer> tuple) throws Exception {
                JQuickRow result = parseGroupKey(tuple.f0, null);
                result.put(resultColumnName, (long) tuple.f1);
                return result;
            }
        });
    }

    @Override
    protected Class<?> getResultType() {
        return Long.class;
    }

    public JQuickComputeTypeImpl getType() {
        return new JQuickFlinkComputeTypeCountImpl();
    }

    private static class JQuickFlinkComputeTypeCountImpl extends JQuickFlinkComputeTypeImpl {
        @Override
        public String getMethod() {
            return JQuickProviderMethodConstants.COUNT;
        }
    }
}
