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

import java.util.List;

/**
 * 将字段转换为 Integer 类型
 *
 * <p>支持的类型转换：
 * <ul>
 *   <li>Number -> Integer</li>
 *   <li>String -> Integer（解析整数）</li>
 *   <li>Boolean -> Integer（true=1, false=0）</li>
 *   <li>null -> null</li>
 * </ul>
 *
 * <p>使用示例：
 * <pre>
 * // 单字段转换
 * ToIntegerProvider provider = new ToIntegerProvider("age", "age_int");
 * JQuickDataSet result = provider.transform(dataSet);
 *
 * // 链式调用
 * JQuickDataSet result = JQuickBaseStandardProvider.transformChain(
 *     dataSet,
 *     new ToIntegerProvider("age", "age_int"),
 *     new ToIntegerProvider("salary", "salary_int")
 * );
 * </pre>
 */
public class JQuickStandardToIntegerProvider extends JQuickBaseStandardProvider<Integer> {

    /**
     * 构造函数 - 单依赖字段
     *
     * @param dependentColumn 依赖的源字段名称
     * @param outputColumnName 输出的新字段名称
     */
    public JQuickStandardToIntegerProvider(String dependentColumn, String outputColumnName) {
        super(dependentColumn, outputColumnName);
    }

    /**
     * 构造函数 - 多依赖字段（取第一个字段的值）
     *
     * @param dependentColumns 依赖的源字段列表
     * @param outputColumnName 输出的新字段名称
     */
    public JQuickStandardToIntegerProvider(List<String> dependentColumns, String outputColumnName) {
        super(dependentColumns, outputColumnName);
    }

    @Override
    protected Integer transform(List<Object> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }

        Object value = values.get(0);
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return ((Number) value).intValue();
        }

        if (value instanceof String) {
            String str = ((String) value).trim();
            if (str.isEmpty()) {
                return null;
            }
            try {
                return Integer.parseInt(str);
            } catch (NumberFormatException e) {
                return null;
            }
        }

        if (value instanceof Boolean) {
            return ((Boolean) value) ? 1 : 0;
        }

        return null;
    }

    @Override
    public Class<Integer> getOutputClass() {
        return Integer.class;
    }

    @Override
    public JQuickComputeTypeImpl getType() {
        return null;
    }
}