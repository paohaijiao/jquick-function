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

import com.github.paohaijiao.provider.standard.JQuickFlinkBaseStandardProvider;
import com.github.paohaijiao.provider.standard.impl.JQuickConcatProvider;
import com.github.paohaijiao.provider.standard.impl.JQuickSumFieldsProvider;
import com.github.paohaijiao.statement.JQuickRow;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Arrays;

/**
 * packageName com.github.paohaijiao
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/6
 */
public class JQuickStandardTest {
    private static JQuickRow createRow(String name, String age, String salary, String bonus, String shipping) {
        JQuickRow row = new JQuickRow();
        row.put("name", name);
        row.put("age", age);
        row.put("salary", salary);
        row.put("bonus", bonus);
        row.put("shipping", shipping);
        return row;
    }

    @Test
    public void test() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<JQuickRow> dataSet = env.fromCollection(Arrays.asList(
                createRow("Alice", "25", "100.5", "50", null),
                createRow("Bob", "30", "200.8", "75", "25"),
                createRow("Charlie", "35", "300.2", "100", "50")
        ));

        // ConcatProvider 示例
        JQuickConcatProvider doubleConcat = new JQuickConcatProvider("name", "age", "name_age", " - ");
        JQuickConcatProvider multiConcat = new JQuickConcatProvider(
                Arrays.asList("name", "age", "salary"),
                "full_info",
                " | "
        );

        // SumFieldsProvider 示例
        JQuickSumFieldsProvider sumBasic = new JQuickSumFieldsProvider(
                Arrays.asList("salary", "bonus"),
                "total"
        );

        // DataSet 链式转换
        DataSet<JQuickRow> result = JQuickFlinkBaseStandardProvider.transformChain(
                dataSet, doubleConcat, multiConcat, sumBasic
        );

        System.out.println("=== DataSet 转换结果 ===");
        result.collect().forEach(System.out::println);

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);

        // 创建 DataStream
        DataStream<JQuickRow> dataStream = streamEnv.fromCollection(
                Arrays.asList(
                        createRow("Alice", "25", "100.5", "50", null),
                        createRow("Bob", "30", "200.8", "75", "25"),
                        createRow("Charlie", "35", "300.2", "100", "50")
                )
        );

        // 方式1：通过 createTemporaryView 创建表
        tableEnv.createTemporaryView(
                "people",
                dataStream,
                Schema.newBuilder()
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.STRING())
                        .column("salary", DataTypes.STRING())
                        .column("bonus", DataTypes.STRING())
                        .column("shipping", DataTypes.STRING())
                        .build()
        );

        // 方式2：直接使用 DataStream 转换
        // 先注册 UDF
        JQuickConcatProvider concatProvider = new JQuickConcatProvider("name", "age", "name_age", " - ");
        concatProvider.registerUDF(tableEnv, "concat_name_age");

        JQuickSumFieldsProvider sumProvider = new JQuickSumFieldsProvider(
                Arrays.asList("salary", "bonus"), "sum_salary_bonus"
        );
        sumProvider.registerUDF(tableEnv, "sum_salary_bonus");

        // 使用 SQL 查询
        Table sqlResult = tableEnv.sqlQuery(
                "SELECT name, age, " +
                        "concat_name_age(name, age) as name_age, " +
                        "sum_salary_bonus(CAST(salary AS DOUBLE), CAST(bonus AS DOUBLE)) as total " +
                        "FROM people"
        );

        System.out.println("=== SQL 查询结果 ===");
        tableEnv.toDataStream(sqlResult, Row.class).print();

        // 执行 Flink 作业
        streamEnv.execute("Flink Provider Example");

    }
}
