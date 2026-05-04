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
package com.github.paohaijiao.extra;

import com.github.paohaijiao.function.JQuickAbstractFunctionService;
import com.github.paohaijiao.spi.anno.Priority;
import com.github.paohaijiao.spi.constants.PriorityConstants;

import java.util.Arrays;
import java.util.stream.Collectors;

@Priority(PriorityConstants.SYSTEM_MEDIUM)
public class JQuickJoinFunctionService extends JQuickAbstractFunctionService {

    private static final long serialVersionUID = 1L;

    public JQuickJoinFunctionService() {
        super("join", "连接多个字符串", String.class);
    }

    @Override
    public Object execute(Object... args) {
        if (args == null || args.length == 0) {
            return "";
        }

        String delimiter = args.length > 1 ? getArg(args, 1, String.class) : "";
        Object[] items = args.length > 0 ? (Object[]) args[0] : new Object[0];

        if (items instanceof Object[]) {
            return Arrays.stream((Object[]) items)
                    .map(String::valueOf)
                    .collect(Collectors.joining(delimiter));
        }
        return String.valueOf(items);
    }
}
