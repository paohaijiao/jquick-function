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
package com.github.paohaijiao.provider.impl;


import com.github.paohaijiao.compute.JQuickComputeTypeImpl;
import com.github.paohaijiao.compute.JQuickJavaComputeTypeImpl;
import com.github.paohaijiao.core.constant.JQuickProviderMethodConstants;
import com.github.paohaijiao.provider.JQuickGroupByAggregationProvider;
import com.github.paohaijiao.statement.JQuickRow;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class JQuickVarianceGroupByProvider extends JQuickGroupByAggregationProvider<Double> {

    private final String varianceColumn;

    private final boolean isSample; // true: 样本方差, false: 总体方差

    public JQuickVarianceGroupByProvider(List<String> groupByColumns, String resultColumnName, String varianceColumn) {
        this(groupByColumns, resultColumnName, varianceColumn, false);
    }

    public JQuickVarianceGroupByProvider(List<String> groupByColumns, String resultColumnName, String varianceColumn, boolean isSample) {
        super(groupByColumns, resultColumnName);
        this.varianceColumn = varianceColumn;
        this.isSample = isSample;
    }

    @Override
    protected Double aggregateGroup(List<JQuickRow> groupRows) {
        List<Double> values = groupRows.stream()
                .map(row -> row.getAs(varianceColumn, Number.class))
                .filter(Objects::nonNull)
                .map(Number::doubleValue)
                .collect(Collectors.toList());
        if (values.isEmpty()) {
            return 0.0;
        }
        double mean = values.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        double sumOfSquares = values.stream()
                .mapToDouble(v -> Math.pow(v - mean, 2))
                .sum();
        int divisor = isSample && values.size() > 1 ? values.size() - 1 : values.size();
        return divisor > 0 ? sumOfSquares / divisor : 0.0;
    }

    @Override
    protected Class<?> getResultType() {
        return Double.class;
    }
    @Override
    public JQuickComputeTypeImpl getType() {
        return new JQuickJavaComputeTypeVarianceImpl();
    }
    private static class JQuickJavaComputeTypeVarianceImpl extends JQuickJavaComputeTypeImpl {
        @Override
        public String getMethod() {
            return JQuickProviderMethodConstants.STDDEV;
        }
    }
}
