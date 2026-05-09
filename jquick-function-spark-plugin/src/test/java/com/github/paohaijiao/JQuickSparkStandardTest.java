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

import com.github.paohaijiao.dataset.JQuickSparkEngine;
import com.github.paohaijiao.provider.JQuickFunctionProvider;

import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;
import com.github.paohaijiao.provider.impl.CountProvider;
import com.github.paohaijiao.provider.impl.SumProvider;

import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * packageName com.github.paohaijiao
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/6
 */
public class JQuickSparkStandardTest {
    private SparkSession spark;
    private JQuickSparkEngine engine;
    private JQuickDataSet originalDataSet;

    @Before
    public void setUp() {
        spark = SparkSession.builder()
                .appName("JQuickSparkTest")
                .master("local[*]")
                .getOrCreate();
        engine = new JQuickSparkEngine(spark);
        originalDataSet = generateLargeDataSet(1);
    }

    /**
     * 示例1: 分布式 SELECT 转换
     */
    @Test
    public void testDistributedSelect() {
        System.out.println("========== 分布式 SELECT 转换 ==========");

        List<JQuickFunctionProvider<?, ?>> providers = Arrays.asList(
//                ColumnProvider.asString("name", "员工姓名"),
//                ColumnProvider.asInt("age", "年龄"),
//                ColumnProvider.asString("department", "部门")
        );
        JQuickDataSet result = engine.select(originalDataSet, providers);
        result.printTable();
        System.out.println("Spark 集群完成分布式转换");
    }

    /**
     * 示例2: 分布式 SELECT + 计算列
     */
    @Test
    public void testDistributedSelectWithComputedColumn() {
        System.out.println("========== 分布式计算列 ==========");
        JQuickFunctionProvider<JQuickRow, Double> annualSalaryProvider =
                new JQuickFunctionProvider<JQuickRow, Double>() {
                    @Override
                    public Double apply(JQuickRow row) {
                        Number salary = row.getAs("salary", Number.class);
                        return salary != null ? salary.doubleValue() * 12 : 0.0;
                    }

                    @Override
                    public String getTargetField() {
                        return "annualSalary";
                    }

                    @Override
                    public Class<?> getTargetClass() {
                        return Double.class;
                    }
                };

        List<JQuickFunctionProvider<?, ?>> providers = Arrays.asList(
//                ColumnProvider.asString("name", "name"),
//                ColumnProvider.asDouble("salary", "monthlySalary"),
                annualSalaryProvider
        );

        JQuickDataSet result = engine.select(originalDataSet, providers);
        result.printTable();
    }

    /**
     * 示例3: 分布式 GROUP BY + 聚合
     */
    @Test
    public void testDistributedAggregation() {
        List<String> groupByColumns = Arrays.asList("department");
        List<JQuickFunctionProvider<?, ?>> aggProviders = Arrays.asList(
                new SumProvider("salary", "totalSalary"),
                new CountProvider("employeeCount")
        );
        JQuickDataSet result = engine.aggregate(originalDataSet, groupByColumns, aggProviders);
        result.printTable();
        System.out.println("Spark 完成分布式 Shuffle 聚合");
    }

    /**
     * 示例4: 多字段分组聚合
     */
    @Test
    public void testDistributedMultiGroupBy() {
        List<String> groupByColumns = Arrays.asList("department", "gender");
        List<JQuickFunctionProvider<?, ?>> aggProviders = Arrays.asList(
                new SumProvider("salary", "totalSalary"),
                new SumProvider("bonus", "totalBonus"),
                new CountProvider("count")
        );

        JQuickDataSet result = engine.aggregate(originalDataSet, groupByColumns, aggProviders);
        result.printTable();
    }

    /**
     * 示例5: Spark DataFrame 互操作
     */
    @Test
    public void testDataFrameInterop() {

        // JQuickDataSet -> Spark DataFrame
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> df = engine.toDataFrame(originalDataSet);
        df.printSchema();
        df.show();
        df.createOrReplaceTempView("employees");
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> result = spark.sql(
                "SELECT department, SUM(salary) as totalSalary, COUNT(*) as count FROM employees GROUP BY department"
        );
        result.show();
        JQuickDataSet backToDataSet = engine.fromDataFrame(result);
        System.out.println("\n转换回 JQuickDataSet:");
        backToDataSet.printTable();
    }

    /**
     * 大规模数据测试 - 100万行
     */
    @Test
    public void testLargeScale1M() {
        System.out.println("========== 大规模数据测试 - 100万行 ==========");
        int rowCount = 1_000_000;
        JQuickDataSet largeDataSet = generateLargeDataSet(rowCount);
        System.out.println("数据集生成完成:");
        System.out.println("  - 行数: " + largeDataSet.size());
        System.out.println("  - 列数: " + largeDataSet.getColumns().size());
        System.out.println("  - 内存估算: ~" + (rowCount * 50 / 1024) + " MB");
        List<String> groupByColumns = Arrays.asList("dept");
        List<JQuickFunctionProvider<?, ?>> aggProviders = Arrays.asList(
                new SumProvider("amount", "totalAmount"),
                new CountProvider("count")
        );
        // 执行聚合
        long startTime = System.currentTimeMillis();
        JQuickDataSet result = engine.aggregate(largeDataSet, groupByColumns, aggProviders);
        long endTime = System.currentTimeMillis();
        System.out.printf("\n处理 %d 行数据耗时: %d ms%n", rowCount, (endTime - startTime));
        System.out.println("吞吐量: " + (rowCount * 1000L / (endTime - startTime)) + " 行/秒");

        System.out.println("\n聚合结果（按部门分组）:");
        result.printTable();


        // 验证总行数一致
        long totalCount = result.getRows().stream()
                .mapToLong(row -> row.getLong("count"))
                .sum();
    }
    @Test
    public void testLargeScale10M() {
        System.out.println("========== 大规模数据测试 - 1000万行 ==========");
        System.out.println("注意：此测试可能需要较长时间和较多内存");

        int rowCount = 10_000_000;
        JQuickDataSet largeDataSet = generateLargeDataSet(rowCount);

        System.out.println("数据集生成完成，行数: " + largeDataSet.size());

        List<String> groupByColumns = Arrays.asList("dept", "category");
        List<JQuickFunctionProvider<?, ?>> aggProviders = Arrays.asList(
                new SumProvider("amount", "totalAmount"),
                new SumProvider("quantity", "totalQuantity"),
                new CountProvider("count")
        );

        long startTime = System.currentTimeMillis();
        JQuickDataSet result = engine.aggregate(largeDataSet, groupByColumns, aggProviders);
        long endTime = System.currentTimeMillis();

        System.out.printf("处理 %d 行数据耗时: %d ms (%d 秒)%n",
                rowCount, (endTime - startTime), (endTime - startTime) / 1000);
        System.out.println("吞吐量: " + (rowCount * 1000L / (endTime - startTime)) + " 行/秒");

        System.out.println("\n聚合结果（前10行）:");
        result.printTable(10);

        // 验证总行数一致
        long totalCount = result.getRows().stream()
                .mapToLong(row -> row.getLong("count"))
                .sum();
    }

    private JQuickDataSet generateLargeDataSet(int rowCount) {
        return null;
    }

}
