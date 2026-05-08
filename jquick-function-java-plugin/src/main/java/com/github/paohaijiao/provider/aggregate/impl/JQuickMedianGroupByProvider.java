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
//package com.github.paohaijiao.provider.aggregate.impl;
//
//import com.github.paohaijiao.compute.JQuickComputeTypeImpl;
//import com.github.paohaijiao.compute.JQuickJavaComputeTypeImpl;
//import com.github.paohaijiao.core.constant.JQuickAggregateProviderMethodConstants;
//import com.github.paohaijiao.provider.aggregate.JQuickJavaGroupByAggregationProvider;
//import com.github.paohaijiao.statement.JQuickRow;
//import java.util.List;
//import java.util.Objects;
//import java.util.stream.Collectors;
//
//public class JQuickMedianGroupByProvider extends JQuickJavaGroupByAggregationProvider<Double> {
//
//    private final String medianColumn;
//
//    public JQuickMedianGroupByProvider(List<String> groupByColumns, String resultColumnName, String medianColumn) {
//        super(groupByColumns, resultColumnName);
//        this.medianColumn = medianColumn;
//    }
//
//    @Override
//    protected Double aggregateGroup(List<JQuickRow> groupRows) {
//        List<Double> values = groupRows.stream()
//                .map(row -> row.getAs(medianColumn, Number.class))
//                .filter(Objects::nonNull)
//                .map(Number::doubleValue)
//                .sorted()
//                .collect(Collectors.toList());
//
//        if (values.isEmpty()) {
//            return 0.0;
//        }
//        int size = values.size();
//        if (size % 2 == 0) {// 偶数个，取中间两个的平均值
//            return (values.get(size / 2 - 1) + values.get(size / 2)) / 2.0;
//        } else {// 奇数个，取中间值
//            return values.get(size / 2);
//        }
//    }
//
//    @Override
//    protected Class<?> getResultType() {
//        return Double.class;
//    }
//
//    @Override
//    public JQuickComputeTypeImpl getType() {
//        return new JQuickJavaComputeTypeMedianImpl();
//    }
//    private static class JQuickJavaComputeTypeMedianImpl extends JQuickJavaComputeTypeImpl {
//        @Override
//        public String getMethod() {
//            return JQuickAggregateProviderMethodConstants.MEDIAN;
//        }
//    }
//}