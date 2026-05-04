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
package com.github.paohaijiao.string;

import com.github.paohaijiao.function.JQuickAbstractFunctionService;
import com.github.paohaijiao.spi.anno.Priority;
import com.github.paohaijiao.spi.constants.PriorityConstants;


@Priority(PriorityConstants.SYSTEM_HIGH)
public class JQuickStringToUpperFunctionService extends JQuickAbstractFunctionService {

    private static final long serialVersionUID = 1L;

    public JQuickStringToUpperFunctionService() {
        super("toUpper", "将字符串转换为大写", String.class);
    }

    @Override
    public Object execute(Object... args) {
        String str = getArg(args, 0, String.class);
        return str.toUpperCase();
    }
}
