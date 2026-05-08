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
package com.github.paohaijiao.provider;

/**
 * packageName com.github.paohaijiao.provider
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/8
 */

import com.github.paohaijiao.statement.JQuickColumnMeta;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;
import com.github.paohaijiao.transform.CountProvider;
import com.github.paohaijiao.transform.SumProvider;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Flink 分布式计算集成
 * 支持批处理（DataSet API）和流处理（DataStream API）
 */
public class JQuickFlinkEngine implements Serializable {

    private final ExecutionEnvironment batchEnv;
    private final StreamExecutionEnvironment streamEnv;

    public JQuickFlinkEngine() {
        this.batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        this.streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    public JQuickFlinkEngine(ExecutionEnvironment batchEnv, StreamExecutionEnvironment streamEnv) {
        this.batchEnv = batchEnv;
        this.streamEnv = streamEnv;
    }
    /**
     * 将 JQuickDataSet 转换为 Flink DataSet
     */
    public DataSet<JQuickRow> toDataSet(JQuickDataSet dataSet) {
        return batchEnv.fromCollection(dataSet.getRows());
    }

    /**
     * 将 Flink DataSet 转换为 JQuickDataSet
     */
    public JQuickDataSet fromDataSet(DataSet<JQuickRow> dataSet, List<JQuickColumnMeta> columns) throws Exception {
        return new JQuickDataSet(columns, dataSet.collect());
    }

    /**
     * 批处理 SELECT 转换
     */
    public DataSet<JQuickRow> selectBatch(DataSet<JQuickRow> input, List<JQuickFunctionProvider<?, ?>> providers) {
        return input.map(new SelectMapFunction(providers));
    }

    /**
     * 批处理 GROUP BY + 聚合
     */
    public DataSet<JQuickRow> aggregateBatch(DataSet<JQuickRow> input, List<String> groupByColumns, List<JQuickFunctionProvider<?, ?>> aggProviders) {
        DataSet<Tuple2<GroupKey, JQuickRow>> grouped = input.map(row -> {
            GroupKey key = new GroupKey(row, groupByColumns);
            JQuickRow partial = new JQuickRow();
            for (String col : groupByColumns) {
                partial.put(col, row.get(col));
            }
            for (JQuickFunctionProvider<?, ?> provider : aggProviders) {
                @SuppressWarnings("unchecked")
                JQuickFunctionProvider<JQuickRow, Object> typedProvider =
                        (JQuickFunctionProvider<JQuickRow, Object>) provider;
                partial.put(provider.getTargetField(), typedProvider.apply(row));
            }
            return new Tuple2<>(key, partial);
        });
        return grouped
                .groupBy(0)
                .reduce(new AggregateReduceFunction(aggProviders))
                .map(tuple -> tuple.f1);
    }

    /**
     * 将 JQuickDataSet 转换为 DataStream
     */
    public DataStream<JQuickRow> toDataStream(JQuickDataSet dataSet) {
        return streamEnv.fromCollection(dataSet.getRows());
    }

    /**
     * 流处理 SELECT 转换
     */
    public DataStream<JQuickRow> selectStream(DataStream<JQuickRow> input, List<JQuickFunctionProvider<?, ?>> providers) {
        return input.map(new SelectMapFunction(providers));
    }

    /**
     * 流处理 GROUP BY + 聚合（滚动聚合）
     * 每来一条数据就输出当前聚合结果
     */
    public DataStream<JQuickRow> aggregateStreamRolling(DataStream<JQuickRow> input, List<String> groupByColumns, List<JQuickFunctionProvider<?, ?>> aggProviders) {
        KeyedStream<JQuickRow, GroupKey> keyedStream = input
                .map(row -> {
                    GroupKey key = new GroupKey(row, groupByColumns);
                    JQuickRow result = new JQuickRow();
                    for (String col : groupByColumns) {
                        result.put(col, row.get(col));
                    }
                    for (JQuickFunctionProvider<?, ?> provider : aggProviders) {
                        @SuppressWarnings("unchecked")
                        JQuickFunctionProvider<JQuickRow, Object> typedProvider =
                                (JQuickFunctionProvider<JQuickRow, Object>) provider;
                        result.put(provider.getTargetField(), typedProvider.apply(row));
                    }
                    return result;
                })
                .keyBy(row -> new GroupKey(row, groupByColumns));
        return keyedStream.process(new RollingAggregateFunction(aggProviders));
    }

    /**
     * 流处理窗口聚合（基于时间窗口）
     */
    public DataStream<JQuickRow> aggregateStreamWindow(DataStream<JQuickRow> input, List<String> groupByColumns, List<JQuickFunctionProvider<?, ?>> aggProviders, long windowSizeMs) {
        return input
                .keyBy(row -> new GroupKey(row, groupByColumns))
                .timeWindow(org.apache.flink.streaming.api.windowing.time.Time.milliseconds(windowSizeMs))
                .aggregate(new WindowAggregateFunction(aggProviders));
    }


    /**
     * SELECT 转换 MapFunction
     */
    private static class SelectMapFunction implements MapFunction<JQuickRow, JQuickRow>, Serializable {
        private final List<JQuickFunctionProvider<?, ?>> providers;

        public SelectMapFunction(List<JQuickFunctionProvider<?, ?>> providers) {
            this.providers = providers;
        }

        @Override
        public JQuickRow map(JQuickRow row) throws Exception {
            JQuickRow result = new JQuickRow();
            for (JQuickFunctionProvider<?, ?> provider : providers) {
                @SuppressWarnings("unchecked")
                JQuickFunctionProvider<JQuickRow, Object> typedProvider =
                        (JQuickFunctionProvider<JQuickRow, Object>) provider;
                result.put(provider.getTargetField(), typedProvider.apply(row));
            }
            return result;
        }
    }

    /**
     * 聚合 ReduceFunction
     */
    private static class AggregateReduceFunction implements ReduceFunction<Tuple2<GroupKey, JQuickRow>>, Serializable {
        private final List<JQuickFunctionProvider<?, ?>> aggProviders;

        public AggregateReduceFunction(List<JQuickFunctionProvider<?, ?>> aggProviders) {
            this.aggProviders = aggProviders;
        }

        @Override
        public Tuple2<GroupKey, JQuickRow> reduce(Tuple2<GroupKey, JQuickRow> t1,
                                                  Tuple2<GroupKey, JQuickRow> t2) {
            JQuickRow merged = new JQuickRow(t1.f1);

            for (JQuickFunctionProvider<?, ?> provider : aggProviders) {
                String field = provider.getTargetField();
                Object v1 = t1.f1.get(field);
                Object v2 = t2.f1.get(field);

                if (provider instanceof SumProvider) {
                    double sum = toDouble(v1) + toDouble(v2);
                    merged.put(field, sum);
                } else if (provider instanceof CountProvider) {
                    long count = toLong(v1) + toLong(v2);
                    merged.put(field, count);
                }
            }

            return new Tuple2<>(t1.f0, merged);
        }

        private double toDouble(Object value) {
            return value instanceof Number ? ((Number) value).doubleValue() : 0.0;
        }

        private long toLong(Object value) {
            return value instanceof Number ? ((Number) value).longValue() : 0L;
        }
    }

    /**
     * 滚动聚合 ProcessFunction
     */
    private static class RollingAggregateFunction
            extends KeyedProcessFunction<GroupKey, JQuickRow, JQuickRow> implements Serializable {

        private final List<JQuickFunctionProvider<?, ?>> aggProviders;
        private transient Map<String, Object> state;

        public RollingAggregateFunction(List<JQuickFunctionProvider<?, ?>> aggProviders) {
            this.aggProviders = aggProviders;
        }

        @Override
        public void open(Configuration parameters) {
            state = new HashMap<>();
        }

        @Override
        public void processElement(JQuickRow value, Context ctx, Collector<JQuickRow> out) {
            for (JQuickFunctionProvider<?, ?> provider : aggProviders) {
                String field = provider.getTargetField();
                Object current = state.get(field);
                @SuppressWarnings("unchecked")
                JQuickFunctionProvider<JQuickRow, Object> typedProvider =
                        (JQuickFunctionProvider<JQuickRow, Object>) provider;
                Object newValue = typedProvider.apply(value);

                if (provider instanceof SumProvider) {
                    double sum = toDouble(current) + toDouble(newValue);
                    state.put(field, sum);
                } else if (provider instanceof CountProvider) {
                    long count = toLong(current) + 1;
                    state.put(field, count);
                }
            }

            // 输出当前聚合结果
            JQuickRow result = new JQuickRow();
            result.putAll(state);
            // 添加分组键字段
            for (java.util.Map.Entry<String, Object> entry : value.entrySet()) {
                result.put(entry.getKey(), entry.getValue());
            }
            out.collect(result);
        }

        private double toDouble(Object value) {
            return value instanceof Number ? ((Number) value).doubleValue() : 0.0;
        }

        private long toLong(Object value) {
            return value instanceof Number ? ((Number) value).longValue() : 0L;
        }
    }

    /**
     * 窗口聚合 AggregateFunction
     */
    private static class WindowAggregateFunction
            implements org.apache.flink.api.common.functions.AggregateFunction<JQuickRow, JQuickRow, JQuickRow>, Serializable {

        private final List<JQuickFunctionProvider<?, ?>> aggProviders;

        public WindowAggregateFunction(List<JQuickFunctionProvider<?, ?>> aggProviders) {
            this.aggProviders = aggProviders;
        }

        @Override
        public JQuickRow createAccumulator() {
            return new JQuickRow();
        }

        @Override
        public JQuickRow add(JQuickRow value, JQuickRow accumulator) {
            for (JQuickFunctionProvider<?, ?> provider : aggProviders) {
                String field = provider.getTargetField();
                Object current = accumulator.get(field);
                @SuppressWarnings("unchecked")
                JQuickFunctionProvider<JQuickRow, Object> typedProvider =
                        (JQuickFunctionProvider<JQuickRow, Object>) provider;
                Object newValue = typedProvider.apply(value);

                if (provider instanceof SumProvider) {
                    double sum = toDouble(current) + toDouble(newValue);
                    accumulator.put(field, sum);
                } else if (provider instanceof CountProvider) {
                    long count = toLong(current) + 1;
                    accumulator.put(field, count);
                }
            }
            return accumulator;
        }

        @Override
        public JQuickRow getResult(JQuickRow accumulator) {
            return accumulator;
        }

        @Override
        public JQuickRow merge(JQuickRow a, JQuickRow b) {
            JQuickRow merged = new JQuickRow(a);
            for (JQuickFunctionProvider<?, ?> provider : aggProviders) {
                String field = provider.getTargetField();
                Object va = a.get(field);
                Object vb = b.get(field);

                if (provider instanceof SumProvider) {
                    merged.put(field, toDouble(va) + toDouble(vb));
                } else if (provider instanceof CountProvider) {
                    merged.put(field, toLong(va) + toLong(vb));
                }
            }
            return merged;
        }

        private double toDouble(Object value) {
            return value instanceof Number ? ((Number) value).doubleValue() : 0.0;
        }

        private long toLong(Object value) {
            return value instanceof Number ? ((Number) value).longValue() : 0L;
        }
    }

    /**
     * 分组键
     */
    private static class GroupKey implements Serializable {
        private final List<Object> values;

        GroupKey(JQuickRow row, List<String> columns) {
            this.values = columns.stream().map(row::get).collect(Collectors.toList());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GroupKey that = (GroupKey) o;
            return Objects.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(values);
        }

        @Override
        public String toString() {
            return values.toString();
        }
    }
}