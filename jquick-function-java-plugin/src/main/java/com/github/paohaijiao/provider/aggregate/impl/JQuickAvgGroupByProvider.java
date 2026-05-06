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
package com.github.paohaijiao.provider.aggregate.impl;

import com.github.paohaijiao.compute.JQuickComputeTypeImpl;
import com.github.paohaijiao.compute.JQuickJavaComputeTypeImpl;
import com.github.paohaijiao.core.constant.JQuickProviderMethodConstants;
import com.github.paohaijiao.provider.aggregate.JQuickJavaGroupByAggregationProvider;
import com.github.paohaijiao.statement.JQuickRow;

import java.util.List;

/**
 * packageName com.github.paohaijiao.provider.impl
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/5
 */
public class JQuickAvgGroupByProvider extends JQuickJavaGroupByAggregationProvider<Double> {

    private final String avgColumn;

    public JQuickAvgGroupByProvider(List<String> groupByColumns, String resultColumnName, String avgColumn) {
        super(groupByColumns, resultColumnName);
        this.avgColumn = avgColumn;
    }

    @Override
    protected Double aggregateGroup(List<JQuickRow> groupRows) {
        return groupRows.stream()
                .mapToDouble(row -> {
                    Number value = row.getAs(avgColumn, Number.class);
                    return value != null ? value.doubleValue() : 0.0;
                })
                .average()
                .orElse(0.0);
    }

    @Override
    protected Class<?> getResultType() {
        return Double.class;
    }


    @Override
    public JQuickComputeTypeImpl getType() {
        return new JQuickJavaComputeTypeAvgImpl();
    }
    private static class JQuickJavaComputeTypeAvgImpl extends JQuickJavaComputeTypeImpl {

        @Override
        public String getMethod() {
            return JQuickProviderMethodConstants.AVG;
        }
    }
}
