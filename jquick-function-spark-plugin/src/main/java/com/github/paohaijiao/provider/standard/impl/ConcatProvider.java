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

import com.github.paohaijiao.compute.JQuickComputeTypeImpl;
import com.github.paohaijiao.provider.standard.JQuickBaseStandardProvider;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import java.util.List;
import java.util.stream.Collectors;

/**
 * packageName com.github.paohaijiao.provider.standard.impl
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/6
 */

public class ConcatProvider extends JQuickBaseStandardProvider<String> {

    private final String delimiter;

    public ConcatProvider(List<String> dependentColumns, String outputColumnName, String delimiter) {
        super(dependentColumns, outputColumnName);
        this.delimiter = delimiter != null ? delimiter : "";
    }

    @Override
    protected String transform(List<Object> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        return values.stream()
                .map(v -> v != null ? v.toString() : "")
                .filter(s -> !s.isEmpty())
                .collect(Collectors.joining(delimiter));
    }

    @Override
    public DataType getSparkDataType() {
        return DataTypes.StringType;
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
