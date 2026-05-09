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
package com.github.paohaijiao.aggregation;

import com.github.paohaijiao.provider.JQuickFunctionProvider;
import com.github.paohaijiao.provider.aggregate.impl.*;
import com.github.paohaijiao.statement.JQuickColumnMeta;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;
import com.github.paohaijiao.transform.aggegate.JQuickAggregateTransformer;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * JQuickAggregateTransformer 聚合查询测试用例
 */
public class JQuickAggregateTransformerTest {

    private JQuickDataSet originalDataSet;

    @Before
    public void setUp() {
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
        originalDataSet = new JQuickDataSet(columns, rows);
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


    @Test
    public void testGroupBySingleColumnSum() {
        List<String> groupByColumns = Arrays.asList("department");
        List<JQuickFunctionProvider<?, ?>> providers = Arrays.asList(
                new JQuickSumProvider("salary", "totalSalary")
        );
        JQuickAggregateTransformer transformer = new JQuickAggregateTransformer(originalDataSet, groupByColumns, providers);
        JQuickDataSet result = transformer.transform();
        result.printTable();
    }

    @Test
    public void testGroupBySingleColumnMultipleAggregations() {
        List<String> groupByColumns = Arrays.asList("department");
        List<JQuickFunctionProvider<?, ?>> providers = Arrays.asList(
                new JQuickSumProvider("salary", "totalSalary"),
                new JQuickSumProvider("bonus", "totalBonus"),
                new JQuickAvgProvider("bonus", "avgBonus"),
                new JQuickMaxProvider("bonus", "maxBonus"),
                new JQuickMinProvider("bonus", "minBonus"),
                new JQuickMedianProvider("bonus", "medianBonus"),
                new JQuickStddevProvider("bonus", "stddevBonus"),
                new JQuickVarianceProvider("bonus", "varianceBonus"),
                new JQuickFirstProvider("bonus", "firstBonus"),
                new JQuickLastProvider("bonus", "lasstBonus"),
                new JQuickCountProvider("employeeCount")
        );
        JQuickAggregateTransformer transformer = new JQuickAggregateTransformer(originalDataSet, groupByColumns, providers);
        JQuickDataSet result = transformer.transform();
        result.printTable();
    }

    /**
     * 多字段分组
     */
    @Test
    public void testGroupByMultipleColumns() {
        List<String> groupByColumns = Arrays.asList("department", "gender");
        List<JQuickFunctionProvider<?, ?>> providers = Arrays.asList(
                new JQuickSumProvider("salary", "totalSalary"),
                new JQuickCountProvider("employeeCount")
        );
        JQuickAggregateTransformer transformer = new JQuickAggregateTransformer(
                originalDataSet, groupByColumns, providers
        );
        JQuickDataSet result = transformer.transform();
        result.printTable();
    }

    /**
     *  无分组 - 全表聚合
     */
    @Test
    public void testAggregateWithoutGroupBy() {
        List<String> groupByColumns = Arrays.asList();
        List<JQuickFunctionProvider<?, ?>> providers = Arrays.asList(
                new JQuickSumProvider("salary", "totalSalary"),
                new JQuickSumProvider("bonus", "totalBonus"),
                new JQuickCountProvider("totalCount")
        );
        JQuickAggregateTransformer transformer = new JQuickAggregateTransformer(
                originalDataSet, groupByColumns, providers
        );
        JQuickDataSet result = transformer.transform();
        result.printTable();
    }

    /**
     *  聚合 + 保留分组字段
     */
    @Test
    public void testAggregateWithMoreGroupFields() {
        List<String> groupByColumns = Arrays.asList("department");
        List<JQuickFunctionProvider<?, ?>> providers = Arrays.asList(
                new JQuickSumProvider("salary", "totalSalary"),
                new JQuickCountProvider("count")
        );

        JQuickAggregateTransformer transformer = new JQuickAggregateTransformer(originalDataSet, groupByColumns, providers);
        JQuickDataSet result = transformer.transform();
        result.printTable();
    }

    /**
     * 验证数据正确性 - 包含类型转换
     */
    @Test
    public void testAggregateWithTypeConversion() {
        List<String> groupByColumns = Arrays.asList("department");
        List<JQuickFunctionProvider<?, ?>> providers = Arrays.asList(
                new JQuickSumProvider("salary", "totalSalary"),
                new JQuickCountProvider("employeeCount")
        );

        JQuickAggregateTransformer transformer = new JQuickAggregateTransformer(originalDataSet, groupByColumns, providers);
        JQuickDataSet result = transformer.transform();
        result.printTable();

    }

    /**
     * 分组键 null 值处理
     */
    @Test
    public void testGroupByWithNullValues() {
        JQuickRow nullRow = new JQuickRow();
        nullRow.put("id", 99);
        nullRow.put("name", "测试");
        nullRow.put("salary", 5000.0);
        nullRow.put("department", null);
        nullRow.put("bonus", 1000.0);
        List<JQuickRow> rowsWithNull = new java.util.ArrayList<>(originalDataSet.getRows());
        rowsWithNull.add(nullRow);
        List<JQuickColumnMeta> columns = originalDataSet.getColumns();
        JQuickDataSet dataSetWithNull = new JQuickDataSet(columns, rowsWithNull);
        List<String> groupByColumns = Arrays.asList("department");
        List<JQuickFunctionProvider<?, ?>> providers = Arrays.asList(
                new JQuickSumProvider("salary", "totalSalary"),
                new JQuickCountProvider("count")
        );
        JQuickAggregateTransformer transformer = new JQuickAggregateTransformer(
                dataSetWithNull, groupByColumns, providers
        );
        JQuickDataSet result = transformer.transform();
        result.printTable();
    }

    /**
     *  大规模数据性能测试（演示用）
     */
    @Test
    public void testPerformanceWithLargeData() {
        List<JQuickColumnMeta> columns = Arrays.asList(
                new JQuickColumnMeta("dept", String.class, "source"),
                new JQuickColumnMeta("amount", Double.class, "source")
        );

        List<JQuickRow> rows = new java.util.ArrayList<>();
        String[] depts = {"A", "B", "C", "D", "E"};
        for (int i = 0; i < 10000; i++) {
            JQuickRow row = new JQuickRow();
            row.put("dept", depts[i % depts.length]);
            row.put("amount", (double) (i % 1000));
            rows.add(row);
        }
        JQuickDataSet largeDataSet = new JQuickDataSet(columns, rows);
        List<String> groupByColumns = Arrays.asList("dept");
        List<JQuickFunctionProvider<?, ?>> providers = Arrays.asList(
                new JQuickSumProvider("amount", "totalAmount"),
                new JQuickCountProvider("count")
        );
        long startTime = System.currentTimeMillis();
        JQuickAggregateTransformer transformer = new JQuickAggregateTransformer(
                largeDataSet, groupByColumns, providers
        );
        JQuickDataSet result = transformer.transform();
        result.printSummary();
    }

    /**
     * 验证聚合函数的累加正确性（逐行验证）
     */
    @Test
    public void testAggregationAccumulation() {
        List<JQuickColumnMeta> columns = Arrays.asList(
                new JQuickColumnMeta("group", String.class, "source"),
                new JQuickColumnMeta("value", Double.class, "source")
        );

        List<JQuickRow> rows = Arrays.asList(
                createRowForTest("A", 10.0),
                createRowForTest("A", 20.0),
                createRowForTest("A", 30.0),
                createRowForTest("B", 5.0),
                createRowForTest("B", 15.0)
        );

        JQuickDataSet testDataSet = new JQuickDataSet(columns, rows);
        List<String> groupByColumns = Arrays.asList("group");
        List<JQuickFunctionProvider<?, ?>> providers = Arrays.asList(
                new JQuickSumProvider("value", "sum"),
                new JQuickCountProvider("count")
        );
        JQuickAggregateTransformer transformer = new JQuickAggregateTransformer(
                testDataSet, groupByColumns, providers
        );
        JQuickDataSet result = transformer.transform();

        result.printTable();

        JQuickRow groupA = result.getRows().stream()
                .filter(r -> "A".equals(r.getString("group")))
                .findFirst()
                .orElse(null);
        assertNotNull(groupA);
        assertEquals(Double.valueOf(60.0), groupA.getDouble("sum"));
        assertEquals(Long.valueOf(3L), groupA.getLong("count"));

        JQuickRow groupB = result.getRows().stream()
                .filter(r -> "B".equals(r.getString("group")))
                .findFirst()
                .orElse(null);
        assertNotNull(groupB);
        assertEquals(Double.valueOf(20.0), groupB.getDouble("sum"));
        assertEquals(Long.valueOf(2L), groupB.getLong("count"));
    }

    private JQuickRow createRowForTest(String group, Double value) {
        JQuickRow row = new JQuickRow();
        row.put("group", group);
        row.put("value", value);
        return row;
    }

    /**
     * 综合示例 - 薪资分析报表
     */
    @Test
    public void testSalaryAnalysisReport() {
        List<String> groupByColumns = Arrays.asList("department");
        List<JQuickFunctionProvider<?, ?>> providers = Arrays.asList(
                new JQuickSumProvider("salary", "总薪资"),
                new JQuickSumProvider("bonus", "总奖金"),
                new JQuickCountProvider("人数"),
                new JQuickSumProvider("salary", "人均薪资")  // 注意：这里实际需要 avg，但当前只有 sum
        );
        JQuickAggregateTransformer transformer = new JQuickAggregateTransformer(
                originalDataSet, groupByColumns, providers
        );
        JQuickDataSet result = transformer.transform();
        System.out.println("\n=== 部门薪资分析报告 ===");
        System.out.println("+----------+------------+------------+------+");
        System.out.println("| 部门     | 总薪资     | 总奖金     | 人数 |");
        System.out.println("+----------+------------+------------+------+");
        for (JQuickRow row : result.getRows()) {
            System.out.printf("| %-8s | %-10.0f | %-10.0f | %-4d |%n",
                    row.getString("department"),
                    row.getDouble("总薪资"),
                    row.getDouble("总奖金"),
                    row.getLong("人数"));
        }
        System.out.println("+----------+------------+------------+------+");

        double totalSalary = 0;
        double totalBonus = 0;
        long totalCount = 0;
        for (JQuickRow row : result.getRows()) {
            totalSalary += row.getDouble("总薪资");
            totalBonus += row.getDouble("总奖金");
            totalCount += row.getLong("人数");
        }
        System.out.printf("\n公司总计: 总薪资=%.0f, 总奖金=%.0f, 总人数=%d%n", totalSalary, totalBonus, totalCount);
        System.out.printf("平均薪资: %.0f%n", totalSalary / totalCount);
    }
}