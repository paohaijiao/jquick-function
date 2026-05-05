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

/**
 * packageName com.github.paohaijiao
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/4
 */

import com.github.paohaijiao.domain.JQuickDataSetFlinkAggregator;
import com.github.paohaijiao.manage.JQuickFunctionProviderManager;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class JQuickFlinkAggregationTest {

    public static void main(String[] args) {
        JQuickDataSet dataset = createSampleDataset();
        System.out.println("=== Flink 批处理聚合 ===");
        JQuickDataSet batchResult = JQuickDataSetFlinkAggregator.on(dataset)
                .groupBy("department")
                .mode("batch")
                .sum("salary", "bonus")
                .avg("age")
                .count("employee_id")
                .execute();
        batchResult.printTable();

        //Table API模式（SQL风格）
        System.out.println("\n=== Flink Table API 聚合 ===");
        JQuickDataSet tableResult = JQuickDataSetFlinkAggregator.on(dataset)
                .groupBy("city")
                .mode("table")
                .sum("salary")
                .avg("age")
                .countDistinct("department")
                .execute();
        tableResult.printTable();

        //流处理模式
        System.out.println("\n=== Flink 流处理聚合 ===");
        JQuickDataSet streamResult = JQuickDataSetFlinkAggregator.on(dataset)
                .groupBy("department", "city")
                .mode("streaming")
                .sum("salary", "bonus")
                .max("salary")
                .min("age")
                .execute();
        streamResult.printTable();

        //使用Map配置
        System.out.println("\n=== 使用Map配置 ===");
        Map<String, List<Object>> aggregations = new LinkedHashMap<>();
        aggregations.put("sum", Arrays.asList("salary"));
        aggregations.put("avg", Arrays.asList("age", "salary"));
        aggregations.put("count", Arrays.asList("employee_id"));
        aggregations.put("count_distinct", Arrays.asList("department"));
        JQuickDataSet mapResult = JQuickDataSetFlinkAggregator.on(dataset).aggregations(aggregations).execute();
        mapResult.printTable();
        // 关闭资源
        JQuickFunctionProviderManager.shutdown();
    }

    private static JQuickDataSet createSampleDataset() {
        JQuickDataSet.Builder builder = JQuickDataSet.builder();
        builder.addColumn("employee_id", Long.class, "system")
                .addColumn("name", String.class, "system")
                .addColumn("department", String.class, "system")
                .addColumn("city", String.class, "system")
                .addColumn("salary", Double.class, "system")
                .addColumn("bonus", Double.class, "system")
                .addColumn("age", Integer.class, "system");

        builder.addRow(createRow(1L, "张三", "技术部", "北京", 15000.0, 5000.0, 28))
                .addRow(createRow(2L, "李四", "技术部", "上海", 18000.0, 6000.0, 32))
                .addRow(createRow(3L, "王五", "市场部", "北京", 12000.0, 3000.0, 25))
                .addRow(createRow(4L, "赵六", "市场部", "上海", 13000.0, 3500.0, 27))
                .addRow(createRow(5L, "钱七", "技术部", "北京", 20000.0, 8000.0, 35))
                .addRow(createRow(6L, "孙八", "财务部", "深圳", 11000.0, 2000.0, 30));

        return builder.build();
    }

    private static JQuickRow createRow(Long id, String name, String dept, String city, Double salary, Double bonus, Integer age) {
        JQuickRow row = new JQuickRow();
        row.put("employee_id", id);
        row.put("name", name);
        row.put("department", dept);
        row.put("city", city);
        row.put("salary", salary);
        row.put("bonus", bonus);
        row.put("age", age);
        return row;
    }
}
