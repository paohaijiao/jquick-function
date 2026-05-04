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

import com.github.paohaijiao.context.JQuickFunctionContext;
import com.github.paohaijiao.function.JQuickFunction;
import com.github.paohaijiao.function.api.JQuickFlinkFunction;
import com.github.paohaijiao.function.api.JQuickSparkFunction;
import com.github.paohaijiao.manage.manager.JQuickFunctionManager;
import com.github.paohaijiao.wapper.JQuickFunctionWrapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

/**
 * packageName com.github.paohaijiao
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/4
 */
public class JQuickFunctionTest {

    @Test
    public void test(){
        JQuickFunction<Integer, Integer> function = x -> x * 2;
        JQuickFunctionContext context = new JQuickFunctionContext();
        Integer result = JQuickFunctionManager.dispatch(function, 10, context);
        System.out.println(result);
    }

    @Test
    public void test1() {
        JQuickSparkFunction<List<Integer>, List<Integer>> function = (sc, data) -> sc.parallelize(data)
                .map((org.apache.spark.api.java.function.Function<Integer, Integer>) x -> x * 2)
                .collect();
        JQuickFunctionContext context = new JQuickFunctionContext();
        SparkConf conf = new SparkConf()
                .setAppName("test")
                .setMaster("local[*]"); // 本地运行
        JavaSparkContext sc = new JavaSparkContext(conf);
        context.put(JavaSparkContext.class, sc);
        List<Integer> data = Arrays.asList(1, 2, 3);
        List<Integer> result =
                JQuickFunctionManager.dispatch(function, data, context);

        System.out.println(result);
    }

    @Test
    public void testFlinkFunction() throws Exception {
        JQuickFlinkFunction<List<Integer>, List<Integer>> function =
                (executionEnv, data) -> {
                    List<Integer> result = new ArrayList<>();
                    try (org.apache.flink.util.CloseableIterator<Integer> it =
                                 executionEnv
                                         .fromCollection(data)
                                         .map((MapFunction<Integer, Integer>) value -> value * 2)
                                         .executeAndCollect()) {

                        while (it.hasNext()) {
                            result.add(it.next());
                        }
                    }

                    return result;
                };
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        JQuickFunctionContext context = new JQuickFunctionContext();
        context.put(StreamExecutionEnvironment.class, env);
        List<Integer> data = Arrays.asList(1, 2, 3);
        Object result = JQuickFunctionManager.dispatch((JQuickFunction) function, data, context);
        System.out.println(result);
    }
    @Test
    public void testFlinkFunction1() throws Exception {
        JQuickFunction<String, String> func = input -> {
            if (input == null) {
                throw new IllegalArgumentException("Input cannot be null");
            }
            return "Hello " + input;
        };
        JQuickFunction<String, String> withFallback = JQuickFunctionWrapper.withFallback(func,
                exception -> {
                    System.err.println("Error: " + exception.getMessage());
                    return "Default value";
                }
        );
        JQuickFunction<String, String> enhanced = JQuickFunctionWrapper
                .builder(func)
                .withFallback(e -> "fallback result")  // Function<Exception, O>
                .withLogging(Logger.getGlobal())
                .build();
        String result = enhanced.apply("nihao");
        System.out.println(result);
    }
}
