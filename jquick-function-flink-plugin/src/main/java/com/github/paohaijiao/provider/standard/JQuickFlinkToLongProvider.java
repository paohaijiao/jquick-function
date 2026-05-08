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
package com.github.paohaijiao.provider.standard;

/**
 * packageName com.github.paohaijiao.provider.standard.impl
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/6
 */


import com.github.paohaijiao.compute.JQuickComputeTypeImpl;
import com.github.paohaijiao.provider.standard.JQuickFlinkBaseStandardProvider;

import java.util.List;

/**
 * 将字段转换为 Long 类型（Flink 版本）
 */
public class JQuickFlinkToLongProvider extends JQuickFlinkBaseStandardProvider<Long> {

    public JQuickFlinkToLongProvider(String dependentColumn, String outputColumnName) {
        super(dependentColumn, outputColumnName);
    }

    public JQuickFlinkToLongProvider(List<String> dependentColumns, String outputColumnName) {
        super(dependentColumns, outputColumnName);
    }

    @Override
    protected Long transform(List<Object> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }

        Object value = values.get(0);
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return ((Number) value).longValue();
        }

        if (value instanceof String) {
            String str = ((String) value).trim();
            if (str.isEmpty()) {
                return null;
            }
            try {
                return Long.parseLong(str);
            } catch (NumberFormatException e) {
                return null;
            }
        }

        if (value instanceof Boolean) {
            return ((Boolean) value) ? 1L : 0L;
        }

        return null;
    }

    @Override
    public Class<Long> getOutputClass() {
        return Long.class;
    }

    @Override
    public JQuickComputeTypeImpl getType() {
        return null;
    }
}
