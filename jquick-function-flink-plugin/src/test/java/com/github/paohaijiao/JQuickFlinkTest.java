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
package com.github.paohaijiao;

import com.github.paohaijiao.provider.JQuickFlinkEngine;
import com.github.paohaijiao.provider.JQuickFunctionProvider;
import com.github.paohaijiao.statement.JQuickColumnMeta;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;
import com.github.paohaijiao.transform.CountProvider;
import com.github.paohaijiao.transform.SumProvider;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * packageName com.github.paohaijiao
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/4
 */
public class JQuickFlinkTest {

    private JQuickFlinkEngine engine;
    private ExecutionEnvironment batchEnv;
    private StreamExecutionEnvironment streamEnv;
    private JQuickDataSet originalDataSet;

    @Before
    public void setUp() {
        batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        engine = new JQuickFlinkEngine(batchEnv, streamEnv);
        originalDataSet = createDataSet();
    }

    /**
     *  Flink 批处理 SELECT
     */
    @Test
    public void testBatchSelect() throws Exception {
        System.out.println("========== Flink 批处理 SELECT ==========");

        DataSet<JQuickRow> input = engine.toDataSet(originalDataSet);

        List<JQuickFunctionProvider<?, ?>> providers = Arrays.asList(
//                ColumnProvider.asString("name", "员工姓名"),
//                ColumnProvider.asInt("age", "年龄"),
//                ColumnProvider.asString("department", "部门")
        );

        DataSet<JQuickRow> result = engine.selectBatch(input, providers);

        List<JQuickRow> rows = result.collect();
        System.out.println("结果行数: " + rows.size());
        rows.forEach(row -> System.out.println(row));
    }

    /**
     *  Flink 批处理 GROUP BY + 聚合
     */
    @Test
    public void testBatchAggregation() throws Exception {
        DataSet<JQuickRow> input = engine.toDataSet(originalDataSet);
        List<String> groupByColumns = Arrays.asList("department");
        List<JQuickFunctionProvider<?, ?>> aggProviders = Arrays.asList(
//                new SumProvider("salary", "totalSalary"),
//                new CountProvider("employeeCount")
        );
        DataSet<JQuickRow> result = engine.aggregateBatch(input, groupByColumns, aggProviders);
        List<JQuickRow> rows = result.collect();
        System.out.println("聚合结果:");
        for (JQuickRow row : rows) {
            System.out.printf("部门: %s, 总薪资: %.0f, 人数: %d%n",
                    row.getString("department"),
                    row.getDouble("totalSalary"),
                    row.getLong("employeeCount"));
        }
    }

    /**
     *  Flink 流处理 SELECT
     */
    @Test
    public void testStreamSelect() throws Exception {
        DataStream<JQuickRow> input = engine.toDataStream(originalDataSet);
        List<JQuickFunctionProvider<?, ?>> providers = Arrays.asList(
//                ColumnProvider.asString("name", "员工姓名"),
//                ColumnProvider.asDouble("salary", "薪资")
        );
        DataStream<JQuickRow> result = engine.selectStream(input, providers);
        // 打印结果（仅打印前5条）
//        result.executeAndCollect(5).forEachRemaining(row ->
//                System.out.printf("姓名: %s, 薪资: %.0f%n",
//                        row.getString("员工姓名"),
//                        row.getDouble("薪资"))
      //  );
    }

    /**
     *  Flink 流处理滚动聚合（实时计算）
     */
    @Test
    public void testStreamRollingAggregation() throws Exception {
        // 使用 DataStream，每条数据都会触发更新
        DataStream<JQuickRow> input = streamEnv.fromElements(
                createRowForStream("技术部", 8000.0),
                createRowForStream("市场部", 10000.0),
                createRowForStream("技术部", 9000.0),
                createRowForStream("销售部", 12000.0),
                createRowForStream("市场部", 6000.0),
                createRowForStream("技术部", 11000.0)
        );

        List<String> groupByColumns = Arrays.asList("department");
        List<JQuickFunctionProvider<?, ?>> aggProviders = Arrays.asList(
                new SumProvider("salary", "runningSum"),
                new CountProvider("runningCount")
        );
        DataStream<JQuickRow> result = engine.aggregateStreamRolling(
                input, groupByColumns, aggProviders);
        result.print();
        streamEnv.execute("Rolling Aggregation Test");
    }

    /**
     * Flink 流处理窗口聚合
     */
    @Test
    public void testStreamWindowAggregation() throws Exception {
        DataStream<JQuickRow> input = streamEnv.fromCollection(
                generateStreamData(100)
        );
        List<String> groupByColumns = Arrays.asList("department");
        List<JQuickFunctionProvider<?, ?>> aggProviders = Arrays.asList(
                new SumProvider("salary", "windowSum"),
                new CountProvider("windowCount")
        );
        DataStream<JQuickRow> result = engine.aggregateStreamWindow(
                input, groupByColumns, aggProviders, 5000L);
        result.print();

        streamEnv.execute("Window Aggregation Test");
    }

    /**
     *  Spark vs Flink 性能对比
     */
    @Test
    public void testPerformanceComparison() throws Exception {
        int rowCount = 100_000;
        JQuickDataSet dataSet = generateLargeDataSet(rowCount);
        DataSet<JQuickRow> flinkInput = engine.toDataSet(dataSet);
        List<String> groupByColumns = Arrays.asList("dept");
        List<JQuickFunctionProvider<?, ?>> aggProviders = Arrays.asList(
                new SumProvider("amount", "totalAmount"),
                new CountProvider("count")
        );
        long flinkStart = System.currentTimeMillis();
        DataSet<JQuickRow> flinkResult = engine.aggregateBatch(flinkInput, groupByColumns, aggProviders);
        List<JQuickRow> flinkRows = flinkResult.collect();
        long flinkEnd = System.currentTimeMillis();
        System.out.printf("Flink 处理 %d 行数据耗时: %d ms%n", rowCount, (flinkEnd - flinkStart));
        System.out.println("Flink 聚合结果:");
        for (JQuickRow row : flinkRows) {
            System.out.printf("  %s: 总额=%.0f, 数量=%d%n",
                    row.getString("dept"),
                    row.getDouble("totalAmount"),
                    row.getLong("count"));
        }
    }

    /**
     * 实时数据流处理示例
     */
    @Test
    public void testRealTimeStreaming() throws Exception {
        // 模拟实时销售数据流
        DataStream<JQuickRow> salesStream = streamEnv.fromElements(
                createSaleEvent("商品A", "电子", 100.0, 1),
                createSaleEvent("商品B", "服装", 200.0, 2),
                createSaleEvent("商品A", "电子", 150.0, 1),
                createSaleEvent("商品C", "食品", 50.0, 5),
                createSaleEvent("商品B", "服装", 180.0, 1),
                createSaleEvent("商品A", "电子", 200.0, 2)
        );
        // 按品类实时统计销售额和销量
        List<String> groupByColumns = Arrays.asList("category");
        List<JQuickFunctionProvider<?, ?>> aggProviders = Arrays.asList(new SumProvider("amount", "totalSales"), new SumProvider("quantity", "totalQuantity"), new CountProvider("transactionCount"));
        DataStream<JQuickRow> result = engine.aggregateStreamRolling(
                salesStream, groupByColumns, aggProviders);
        result.print();
        streamEnv.execute("Real-time Sales Analysis");
    }

    private JQuickDataSet createDataSet() {
        List<JQuickColumnMeta> columns = Arrays.asList(
                new JQuickColumnMeta("id", Integer.class, "source"),
                new JQuickColumnMeta("name", String.class, "source"),
                new JQuickColumnMeta("age", Integer.class, "source"),
                new JQuickColumnMeta("salary", Double.class, "source"),
                new JQuickColumnMeta("department", String.class, "source"),
                new JQuickColumnMeta("bonus", Double.class, "source"),
                new JQuickColumnMeta("gender", String.class, "source")
        );

        List<JQuickRow> rows = Arrays.asList(
                createRow(1, "张三", 25, 8000.0, "技术部", 2000.0, "男"),
                createRow(2, "李四", 30, 10000.0, "市场部", 3000.0, "女"),
                createRow(3, "王五", 28, 9000.0, "技术部", 2500.0, "男"),
                createRow(4, "赵六", 35, 12000.0, "销售部", 4000.0, "男"),
                createRow(5, "钱七", 22, 6000.0, "市场部", 1500.0, "女"),
                createRow(6, "孙八", 32, 11000.0, "技术部", 3500.0, "女"),
                createRow(7, "周九", 29, 9500.0, "销售部", 2800.0, "女")
        );

        return new JQuickDataSet(columns, rows);
    }

    private JQuickRow createRow(Object... values) {
        JQuickRow row = new JQuickRow();
        row.put("id", values[0]);
        row.put("name", values[1]);
        row.put("age", values[2]);
        row.put("salary", values[3]);
        row.put("department", values[4]);
        row.put("bonus", values[5]);
        row.put("gender", values[6]);
        return row;
    }

    private JQuickRow createRowForStream(String department, Double salary) {
        JQuickRow row = new JQuickRow();
        row.put("department", department);
        row.put("salary", salary);
        row.put("timestamp", System.currentTimeMillis());
        return row;
    }

    private JQuickRow createSaleEvent(String product, String category, Double amount, Integer quantity) {
        JQuickRow row = new JQuickRow();
        row.put("product", product);
        row.put("category", category);
        row.put("amount", amount);
        row.put("quantity", quantity);
        row.put("timestamp", System.currentTimeMillis());
        return row;
    }

    private JQuickDataSet generateLargeDataSet(int rowCount) {
        List<JQuickColumnMeta> columns = Arrays.asList(
                new JQuickColumnMeta("id", Long.class, "generated"),
                new JQuickColumnMeta("dept", String.class, "generated"),
                new JQuickColumnMeta("amount", Double.class, "generated")
        );

        String[] depts = {"技术部", "市场部", "销售部", "人事部", "财务部"};
        Random random = new Random();
        List<JQuickRow> rows = new ArrayList<>(rowCount);

        for (int i = 0; i < rowCount; i++) {
            JQuickRow row = new JQuickRow();
            row.put("id", (long) i);
            row.put("dept", depts[random.nextInt(depts.length)]);
            row.put("amount", 1000 + random.nextDouble() * 9000);
            rows.add(row);
        }

        return new JQuickDataSet(columns, rows);
    }

    private List<JQuickRow> generateStreamData(int count) {
        List<JQuickRow> rows = new ArrayList<>();
        String[] depts = {"技术部", "市场部", "销售部"};
        Random random = new Random();

        for (int i = 0; i < count; i++) {
            JQuickRow row = new JQuickRow();
            row.put("department", depts[random.nextInt(depts.length)]);
            row.put("salary", 5000 + random.nextDouble() * 15000);
            row.put("timestamp", System.currentTimeMillis());
            rows.add(row);
        }
        return rows;
    }

    private static class Random extends java.util.Random {
        public Random() {
            super(42);
        }
    }
}
