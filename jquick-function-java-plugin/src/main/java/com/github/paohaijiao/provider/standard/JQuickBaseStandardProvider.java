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


import com.github.paohaijiao.compute.JQuickComputeTypeImpl;
import com.github.paohaijiao.provider.JStandardProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 抽象标准转换器基类 - 输入 List&lt;Object&gt;（多字段），输出单个值 R
 *
 * <p>子类只需实现 {@link #transform(List)} 方法即可
 *
 * @param <R> 输出类型
 */
public abstract class JQuickBaseStandardProvider<R> implements JStandardProvider<Object, R> {

    /** 依赖的源字段列表 */
    protected final List<String> dependentColumns;

    /** 输出的新字段名称 */
    protected final String outputColumnName;

    /**
     * 构造函数 - 多依赖字段
     *
     * @param dependentColumns 依赖的源字段列表
     * @param outputColumnName 输出的新字段名称
     */
    public JQuickBaseStandardProvider(List<String> dependentColumns, String outputColumnName) {
        this.dependentColumns = dependentColumns != null ? new ArrayList<>(dependentColumns) : Collections.emptyList();
        this.outputColumnName = outputColumnName;
    }

    /**
     * 构造函数 - 单依赖字段
     *
     * @param dependentColumn 依赖的源字段
     * @param outputColumnName 输出的新字段名称
     */
    public JQuickBaseStandardProvider(String dependentColumn, String outputColumnName) {
        this(Collections.singletonList(dependentColumn), outputColumnName);
    }


    @Override
    public R apply(List<Object> values) {
        // 输入 List<Object>（多字段值），输出单个值 R
        if (values == null || values.isEmpty()) {
            return onEmptyValues();
        }
        return transform(values);
    }

    /**
     * 核心转换方法 - 将输入值列表转换为单个输出值（子类必须实现）
     *
     * @param values 依赖字段的值列表，顺序与 dependentColumns 一致
     * @return 转换后的值
     */
    protected abstract R transform(List<Object> values);

    /**
     * 当输入值为 null 或空时的处理策略
     * 子类可覆盖此方法自定义行为
     *
     * @return 默认返回 null
     */
    protected R onEmptyValues() {
        return null;
    }

    @Override
    public List<String> getDependentColumns() {
        return dependentColumns;
    }

    @Override
    public String getOutputColumnName() {
        return outputColumnName;
    }

    @Override
    public boolean skipOnNull() {
        return true;
    }
}