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
package com.github.paohaijiao.math;

/**
 * packageName com.github.paohaijiao.math
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/4
 */

import com.github.paohaijiao.function.JQuickAbstractFunctionService;
import com.github.paohaijiao.spi.anno.Priority;
import com.github.paohaijiao.spi.constants.PriorityConstants;

@Priority(PriorityConstants.SYSTEM_HIGH)
public class JQuickMathSumFunctionService extends JQuickAbstractFunctionService {

    private static final long serialVersionUID = 1L;

    public JQuickMathSumFunctionService() {
        super("sum", "计算数字列表的总和", Number.class);
    }

    @Override
    public Object execute(Object... args) {
        if (args == null || args.length == 0) {
            return 0;
        }
        double sum = 0;
        for (Object arg : args) {
            if (arg instanceof Number) {
                sum += ((Number) arg).doubleValue();
            } else {
                throw new IllegalArgumentException("参数必须是数字类型: " + arg);
            }
        }
        return sum;
    }

    @Override
    public boolean validateArgs(Object... args) {
        if (args == null) return true;
        for (Object arg : args) {
            if (arg != null && !(arg instanceof Number)) {
                return false;
            }
        }
        return true;
    }
}
