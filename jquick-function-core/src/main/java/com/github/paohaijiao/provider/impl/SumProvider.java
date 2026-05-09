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

import com.github.paohaijiao.statement.JQuickRow;
import com.github.paohaijiao.provider.JQuickAbstractAggregationProvider;

/**
 * packageName com.github.paohaijiao.provider.aggregate.impl
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/8
 */
public  class SumProvider extends JQuickAbstractAggregationProvider<Double> {

    public SumProvider(String sourceColumn, String targetField) {
        super(sourceColumn, targetField, Double.class);
    }

    @Override
    public Double getInitialValue() {
        return 0.0;
    }

    @Override
    public Double apply(JQuickRow row) {
        Number value = row.getAs(sourceColumn, Number.class);
        return value != null ? value.doubleValue() : 0.0;
    }

    @Override
    public Double accumulate(Double current, Double next) {
        return current + next;
    }

    @Override
    public Double merge(Double a, Double b) {
        return a + b;
    }
}
