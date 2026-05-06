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
package com.github.paohaijiao.aggregation;


import com.github.paohaijiao.provider.aggregate.impl.JQuickSumGroupByProvider;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * packageName com.github.paohaijiao.aggregation
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/5
 */
public class JQuickAggregationTest {

    @Test
    public void test() {
        JQuickRow row1 = new JQuickRow();
        row1.put("class", "1班");
        row1.put("score", 95);
        JQuickRow row2 = new JQuickRow();
        row2.put("class", "1班");
        row2.put("score", 47);

        JQuickRow row3 = new JQuickRow();
        row3.put("class", "2班");
        row3.put("score", 100);
        JQuickRow row4 = new JQuickRow();
        row4.put("class", "2班");
        row4.put("score", 34);
        List<JQuickRow> rows = Arrays.asList(row1, row2, row3, row4);
        // 对class 进行聚合 ,计算字段是score 采用sum 的方式，最后形成新字段total_score
        JQuickSumGroupByProvider sumProvider = new JQuickSumGroupByProvider(
                Arrays.asList("class"),
                "total_score",
                "score"
        );

        JQuickDataSet result = sumProvider.aggregate(rows);
        result.printTable();
    }
}
