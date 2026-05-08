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

import com.github.paohaijiao.statement.JQuickRow;

/**
 * packageName com.github.paohaijiao.provider.aggregate.impl
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/8
 */
public class CountProvider extends AbstractAggregationProvider<Long> {
    public CountProvider(String targetField) {
        super(null, targetField, Long.class);
    }

    @Override
    public Long apply(JQuickRow row) {
        return 1L;
    }

    @Override
    public Long accumulate(Long current, Long next) {
        return current + next;
    }
}
