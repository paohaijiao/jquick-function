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
import java.util.Objects;

public class JQuickMaxGroupByProvider extends JQuickJavaGroupByAggregationProvider<Object> {

    private final String maxColumn;

    public JQuickMaxGroupByProvider(List<String> groupByColumns, String resultColumnName, String maxColumn) {
        super(groupByColumns, resultColumnName);
        this.maxColumn = maxColumn;
    }

    @Override
    protected Object aggregateGroup(List<JQuickRow> groupRows) {
        return groupRows.stream()
                .map(row -> row.get(maxColumn))
                .filter(Objects::nonNull)
                .max((a, b) -> {
                    if (a instanceof Comparable && b instanceof Comparable) {
                        return ((Comparable) a).compareTo(b);
                    }
                    return a.toString().compareTo(b.toString());
                })
                .orElse(null);
    }

    @Override
    protected Class<?> getResultType() {
        return Object.class;
    }


    @Override
    public JQuickComputeTypeImpl getType() {
        return new JQuickJavaComputeTypeMaxImpl();
    }
    private static class JQuickJavaComputeTypeMaxImpl extends JQuickJavaComputeTypeImpl {

        @Override
        public String getMethod() {
            return JQuickProviderMethodConstants.MAX;
        }
    }
}
