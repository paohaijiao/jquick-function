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
package com.github.paohaijiao.extra;

/**
 * packageName com.github.paohaijiao.extra
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/6
 */

import com.github.paohaijiao.provider.standard.JQuickFlinkBaseStandardProvider;
import com.github.paohaijiao.statement.JQuickRow;
import org.apache.flink.api.common.functions.MapFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Flink MapFunction - 添加新列并删除原始依赖列
 */
public class StandardProviderDropColumnsMapFunction<R> implements MapFunction<JQuickRow, JQuickRow>, Serializable {

    private static final long serialVersionUID = 1L;

    private final JQuickFlinkBaseStandardProvider<R> provider;

    public StandardProviderDropColumnsMapFunction(JQuickFlinkBaseStandardProvider<R> provider) {
        this.provider = provider;
    }

    @Override
    public JQuickRow map(JQuickRow row) throws Exception {
        JQuickRow newRow = new JQuickRow(row);

        List<Object> values = new ArrayList<>();
        for (String col : provider.getDependentColumns()) {
            values.add(row.get(col));
        }

        R result = provider.apply(values);

        for (String col : provider.getDependentColumns()) {
            newRow.remove(col);
        }

        newRow.put(provider.getOutputColumnName(), result);

        return newRow;
    }
}

