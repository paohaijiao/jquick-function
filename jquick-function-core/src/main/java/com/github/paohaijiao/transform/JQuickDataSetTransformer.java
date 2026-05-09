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
package com.github.paohaijiao.transform;


import com.github.paohaijiao.provider.JQuickFunctionProvider;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;
import com.github.paohaijiao.statement.JQuickColumnMeta;

import java.util.*;

/**
 * 数据集转换抽象基类
 * 迭代每一行数据，根据 JStandardProvider 进行字段转换，最后组装成新的 DataSet
 *
 * @author Martin
 * @since 1.0.0
 */
public abstract class JQuickDataSetTransformer {

    protected JQuickDataSet inputDataSet;

    protected List<JQuickRow> transformedRows;

    protected List<JQuickColumnMeta> transformedColumns=new ArrayList<>();

    protected  List<JQuickFunctionProvider<?,?>> providers =new ArrayList<>();

    public JQuickDataSetTransformer(JQuickDataSet inputDataSet,List< JQuickFunctionProvider<?, ?>> jquickFieldMappings) {
        this.inputDataSet = inputDataSet;
        this.transformedRows = new ArrayList<>();
        this.providers.addAll(jquickFieldMappings);
        for (JQuickFunctionProvider<?,?> provider : jquickFieldMappings) {
            transformedColumns.add(new JQuickColumnMeta(provider.getTargetField(),provider.getTargetClass(),""));
        }
        for (JQuickRow row : inputDataSet.getRows()) {
            JQuickRow newRow = new JQuickRow();
            newRow.putAll(row);
            this.transformedRows.add(newRow);
        }
    }

    /**
     * 执行转换 - 核心方法
     * 迭代每一行，应用所有字段映射转换
     *
     * @return 转换后的数据集
     */
    public JQuickDataSet transform() {
        preProcess();
        List<JQuickRow> originalRows = inputDataSet.getRows();
        for (int i = 0; i < transformedRows.size(); i++) {
            JQuickRow targetRow = transformedRows.get(i);
            JQuickRow sourceRow = originalRows.get(i);
            transformRow(sourceRow, targetRow, providers, i);
        }
        postProcess();
        return buildDataSet();
    }




    /**
     * 预处理方法（子类可覆盖）
     */
    protected void preProcess() {
    }

    protected abstract void transformRow(JQuickRow sourceRow, JQuickRow targetRow, List<JQuickFunctionProvider<?, ?>> fieldMappings, int rowIndex);
    /**
     * 后处理方法（子类可覆盖）
     */
    protected void postProcess() {
    }

    /**
     * 组装新的数据集
     *
     * @return 新数据集
     */
    protected JQuickDataSet buildDataSet() {
        return new JQuickDataSet(transformedColumns, transformedRows);
    }

    /**
     * 添加新的字段映射
     */
    public JQuickDataSetTransformer addMapping(JQuickFunctionProvider<?, ?> mapping) {
        this.providers.add(mapping);
        return this;
    }

    /**
     * 批量添加字段映射
     */
    public JQuickDataSetTransformer addMappings(List<JQuickFunctionProvider<?, ?>> mappings) {
        this.providers.addAll(mappings);
        return this;
    }
    /**
     * 获取当前行数
     */
    public int getRowCount() {
        return transformedRows.size();
    }

    /**
     * 获取当前列数
     */
    public int getColumnCount() {
        return transformedColumns.size();
    }

    /**
     * 获取原始数据集
     */
    public JQuickDataSet getInputDataSet() {
        return inputDataSet;
    }
    /**
     * 获取转换后的行数据（只读）
     */
    public List<JQuickRow> getTransformedRows() {
        return Collections.unmodifiableList(transformedRows);
    }

}