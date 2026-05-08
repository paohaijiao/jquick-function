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
//import com.github.paohaijiao.manage.JQuickFunctionProviderManager;
//import com.github.paohaijiao.provider.JQuickFunctionProvider;
//import org.junit.Test;
//
//import java.util.Arrays;
//import java.util.List;
//import java.util.Optional;
//
///**
// * packageName com.github.paohaijiao
// *
// * @author Martin
// * @version 1.0.0
// * @since 2026/5/5
// */
//public class JQuickSparkProviderTest {
//    @Test
//    public void test() {
//        JQuickFunctionProviderManager manager = JQuickFunctionProviderManager.getInstance();
//        Object result1 = manager.executeJavaByMethodOne("toString", 123);
//        Object result2 = manager.executeJavaByMethodTwo("add", 10, 20);
//        Object result3 = manager.executeJavaByMethodThree("format", "%s-%s", "hello", "world");
//        List<Object> list = Arrays.asList(1, 2, 3, 4, 5);
//        Object result4 = manager.executeJavaByMethod("sum", list);
//        Object sumResult = manager.executeSparkByMethodOne("sum", 100);
//        Object avgResult = manager.executeSparkByMethodTwo("avg", 100, 200);
//        Object rangeResult = manager.executeSparkByMethodThree("range", 1, 10, 2);
//        List<Object> numbers = Arrays.asList(1, 2, 3, 4, 5);
//        Object countResult = manager.executeSparkByMethod("count", numbers);
//        Object windowResult = manager.executeFlinkByMethodOne("window", "tumble");
//      //  Object joinResult = manager.executeFlinkByMethodTwo("join", stream1, stream2);
//        Object aggregateResult = manager.executeFlinkByMethodThree("aggregate", "sum", "field", "window");
//        List<Object> data = Arrays.asList("a", "b", "c");
//        Object filterResult = manager.executeFlinkByMethod("filter", data);
//        List<String> javaMethods = manager.getAvailableJavaMethodNames();
//        List<String> sparkMethods = manager.getAvailableSparkMethodNames();
//        List<String> flinkMethods = manager.getAvailableFlinkMethodNames();
//
//        System.out.println("Java方法: " + javaMethods);
//        System.out.println("Spark方法: " + sparkMethods);
//        System.out.println("Flink方法: " + flinkMethods);
//
//        Optional<JQuickFunctionProvider> sparkProvider = manager.getSparkProviderByMethod("max");
//        sparkProvider.ifPresent(provider -> {
//            Object maxResult = provider.applyOne(100);
//        });
//        manager.printAllProvidersInfo();
//        System.out.println(manager.getMethodStatistics());
//    }
//}
