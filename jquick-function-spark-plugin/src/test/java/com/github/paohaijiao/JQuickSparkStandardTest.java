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

import com.github.paohaijiao.provider.standard.JQuickBaseStandardProvider;
import com.github.paohaijiao.provider.standard.impl.JQuickSparkConcatProvider;
import com.github.paohaijiao.provider.standard.impl.JQuickSparkToIntegerProvider;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.Arrays;

/**
 * packageName com.github.paohaijiao
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/6
 */
public class JQuickSparkStandardTest {
    @Test
    public void test() {
        SparkSession spark = SparkSession.builder()
                .appName("example")
                .master("local[*]")
                .getOrCreate();
        Dataset<Row> df = spark.createDataFrame(
                Arrays.asList(
                        RowFactory.create("Alice", "25", "100", "50"),
                        RowFactory.create("Bob", "30", "200", "75")
                ),
                StructType.fromDDL("name STRING, age STRING, salary STRING, bonus STRING")
        );

        // 单个转换
        JQuickSparkToIntegerProvider ageToInt = new JQuickSparkToIntegerProvider("age", "age_int");
        Dataset<Row> df1 = ageToInt.transform(df);
        // 链式转换
        JQuickSparkConcatProvider nameConcat = new JQuickSparkConcatProvider(Arrays.asList("name", "age_int"), "name_age", " - ");
        Dataset<Row> df2 = nameConcat.transform(df1);
        Dataset<Row> result = JQuickBaseStandardProvider.transformChain(df, ageToInt, nameConcat);
        result.show();
    }
}
