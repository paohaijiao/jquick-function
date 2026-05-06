///*
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// *
// * Copyright (c) [2025-2099] Martin (goudingcheng@gmail.com)
// */
//package com.github.paohaijiao;
//
//import com.github.paohaijiao.context.JQuickFunctionContext;
//import com.github.paohaijiao.function.JQuickSparkFunction;
//import com.github.paohaijiao.manage.JQuickFunctionProviderManager;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.junit.Test;
//
//import java.util.Arrays;
//import java.util.List;
//
///**
// * packageName com.github.paohaijiao
// *
// * @author Martin
// * @version 1.0.0
// * @since 2026/5/4
// */
//public class JQuickSparkTest {
//
//    @Test
//    public void test1() {
//        JQuickSparkFunction<List<Integer>, List<Integer>> function = (sc, data) -> sc.parallelize(data)
//                .map((org.apache.spark.api.java.function.Function<Integer, Integer>) x -> x * 2)
//                .collect();
//        JQuickFunctionContext context = new JQuickFunctionContext();
//        SparkConf conf = new SparkConf()
//                .setAppName("test")
//                .setMaster("local[*]"); // 本地运行
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        context.put(JavaSparkContext.class, sc);
//        List<Integer> data = Arrays.asList(1, 2, 3);
//        List<Integer> result = JQuickFunctionProviderManager.dispatch(function, data, context);
//        System.out.println(result);
//    }
//}
