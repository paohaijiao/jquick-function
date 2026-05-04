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
package com.github.paohaijiao.domain;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 聚合函数配置
 * 支持一个函数应用到多列
 */
public class JQuickAggregationDomain implements Serializable {

    private final String functionName;

    private final List<String> columns;

    private final Map<String, String> columnAliases;

    public JQuickAggregationDomain(String functionName, List<String> columns) {
        this.functionName = functionName;
        this.columns = new ArrayList<>(columns);
        this.columnAliases = new HashMap<>();
        for (String col : columns) {
            this.columnAliases.put(col, functionName + "_" + col);
        }
    }

    public JQuickAggregationDomain(String functionName, List<String> columns, Map<String, String> columnAliases) {
        this.functionName = functionName;
        this.columns = new ArrayList<>(columns);
        this.columnAliases = new HashMap<>(columnAliases);
    }

    public String getFunctionName() {
        return functionName;
    }

    public List<String> getColumns() {
        return columns;
    }

    public Map<String, String> getColumnAliases() {
        return columnAliases;
    }

    /**
     * 为指定列设置别名
     */
    public JQuickAggregationDomain withAlias(String column, String alias) {
        this.columnAliases.put(column, alias);
        return this;
    }

    /**
     * 获取所有聚合列的别名列表
     */
    public List<String> getAllAliases() {
        return columns.stream().map(col -> columnAliases.getOrDefault(col, functionName + "_" + col)).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return String.format("%s(%s)", functionName, String.join(", ", columns));
    }
}