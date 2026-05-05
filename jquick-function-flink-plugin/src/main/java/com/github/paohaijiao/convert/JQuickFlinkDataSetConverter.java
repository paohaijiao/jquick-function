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
package com.github.paohaijiao.convert;

import com.github.paohaijiao.statement.JQuickRow;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;


import java.util.List;
import java.util.Objects;

/**
 * Flink数据集转换器
 */
public class JQuickFlinkDataSetConverter {

    /**
     * 将JQuickRow列表转换为Flink DataSet
     */
    public static DataSet<JQuickRow> toFlinkDataSet(List<JQuickRow> rows, ExecutionEnvironment env) {
        return env.fromCollection(rows);
    }

    /**
     * 将Flink DataSet转换为JQuickRow列表
     */
    public static List<JQuickRow> toJQuickRowList(DataSet<JQuickRow> dataSet) throws Exception {
        return dataSet.collect();
    }

    /**
     * 创建分组键
     */
    public static class GroupKey {

        private final List<Object> values;

        public GroupKey(List<Object> values) {
            this.values = values;
        }

        public List<Object> getValues() {
            return values;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GroupKey groupKey = (GroupKey) o;
            return Objects.equals(values, groupKey.values);
        }

        @Override
        public int hashCode() {
            return Objects.hash(values);
        }
    }
}
