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
package com.github.paohaijiao.provider.standard.impl;

/**
 * packageName com.github.paohaijiao.provider.standard
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/6
 */

import com.github.paohaijiao.compute.JQuickComputeTypeImpl;
import com.github.paohaijiao.provider.standard.JQuickFlinkBaseStandardProvider;

import java.util.List;

/**
 * 将字段转换为 String 类型（Flink 版本）
 */
public class JQuickToStringProvider extends JQuickFlinkBaseStandardProvider<String> {

    private final String nullReplacement;

    public JQuickToStringProvider(String dependentColumn, String outputColumnName) {
        this(dependentColumn, outputColumnName, null);
    }

    public JQuickToStringProvider(String dependentColumn, String outputColumnName, String nullReplacement) {
        super(dependentColumn, outputColumnName);
        this.nullReplacement = nullReplacement;
    }

    public JQuickToStringProvider(List<String> dependentColumns, String outputColumnName) {
        this(dependentColumns, outputColumnName, null);
    }

    public JQuickToStringProvider(List<String> dependentColumns, String outputColumnName, String nullReplacement) {
        super(dependentColumns, outputColumnName);
        this.nullReplacement = nullReplacement;
    }

    @Override
    protected String transform(List<Object> values) {
        if (values == null || values.isEmpty()) {
            return nullReplacement;
        }

        Object value = values.get(0);
        if (value == null) {
            return nullReplacement;
        }

        return value.toString();
    }

    @Override
    public Class<String> getOutputClass() {
        return String.class;
    }

    @Override
    public JQuickComputeTypeImpl getType() {
        return null;
    }
}
