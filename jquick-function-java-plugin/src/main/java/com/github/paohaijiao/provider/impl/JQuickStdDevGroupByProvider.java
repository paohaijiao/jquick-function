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
import com.github.paohaijiao.provider.JQuickJavaGroupByAggregationProvider;
import com.github.paohaijiao.statement.JQuickRow;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class JQuickStdDevGroupByProvider extends JQuickJavaGroupByAggregationProvider<Double> {

    private final String stddevColumn;

    private final boolean isSample; // true: 样本标准差, false: 总体标准差

    public JQuickStdDevGroupByProvider(List<String> groupByColumns, String resultColumnName, String stddevColumn) {
        this(groupByColumns, resultColumnName, stddevColumn, false);
    }

    public JQuickStdDevGroupByProvider(List<String> groupByColumns, String resultColumnName, String stddevColumn, boolean isSample) {
        super(groupByColumns, resultColumnName);
        this.stddevColumn = stddevColumn;
        this.isSample = isSample;
    }

    @Override
    protected Double aggregateGroup(List<JQuickRow> groupRows) {
        List<Double> values = groupRows.stream()
                .map(row -> row.getAs(stddevColumn, Number.class))
                .filter(Objects::nonNull)
                .map(Number::doubleValue)
                .collect(Collectors.toList());
        if (values.size() < 2) {
            return 0.0;
        }
        double mean = values.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        double sumOfSquares = values.stream()
                .mapToDouble(v -> Math.pow(v - mean, 2))
                .sum();
        int divisor = isSample ? values.size() - 1 : values.size();
        double variance = sumOfSquares / divisor;
        return Math.sqrt(variance);
    }

    @Override
    protected Class<?> getResultType() {
        return Double.class;
    }
    @Override
    public JQuickComputeTypeImpl getType() {
        return new JQuickJavaComputeTypeMinImpl();
    }
    private static class JQuickJavaComputeTypeMinImpl extends JQuickJavaComputeTypeImpl {
        @Override
        public String getMethod() {
            return JQuickProviderMethodConstants.STDDEV;
        }
    }
}
