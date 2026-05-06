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

/**
 * packageName com.github.paohaijiao.provider.impl
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/5
 */

import com.github.paohaijiao.compute.JQuickComputeTypeImpl;
import com.github.paohaijiao.compute.JQuickJavaComputeTypeImpl;
import com.github.paohaijiao.core.constant.JQuickProviderMethodConstants;
import com.github.paohaijiao.provider.aggregate.JQuickJavaGroupByAggregationProvider;
import com.github.paohaijiao.statement.JQuickRow;
import java.util.List;

public class JQuickLastGroupByProvider extends JQuickJavaGroupByAggregationProvider<Object> {

    private final String lastColumn;

    public JQuickLastGroupByProvider(List<String> groupByColumns, String resultColumnName, String lastColumn) {
        super(groupByColumns, resultColumnName);
        this.lastColumn = lastColumn;
    }

    @Override
    protected Object aggregateGroup(List<JQuickRow> groupRows) {
        if (groupRows == null || groupRows.isEmpty()) {
            return null;
        }
        return groupRows.get(groupRows.size() - 1).get(lastColumn);
    }

    @Override
    protected Class<?> getResultType() {
        return Object.class;
    }

    @Override
    public JQuickComputeTypeImpl getType() {
        return new JQuickJavaComputeTypeLastImpl();
    }
    private static class JQuickJavaComputeTypeLastImpl extends JQuickJavaComputeTypeImpl {
        @Override
        public String getMethod() {
            return JQuickProviderMethodConstants.LAST;
        }
    }
}
