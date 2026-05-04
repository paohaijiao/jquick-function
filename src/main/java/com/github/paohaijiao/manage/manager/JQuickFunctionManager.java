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
package com.github.paohaijiao.manage.manager;

import com.github.paohaijiao.context.JQuickFunctionContext;
import com.github.paohaijiao.executor.JQuickFunctionExecutor;
import com.github.paohaijiao.function.JQuickFunction;
import com.github.paohaijiao.spi.ServiceLoader;

import java.util.List;

/**
 * packageName com.github.paohaijiao.manage
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/4
 */
public class JQuickFunctionManager {

    public static <I, O> O dispatch(JQuickFunction<I, O> function, I input, JQuickFunctionContext context) {
        List<JQuickFunctionExecutor> executors = ServiceLoader.loadServicesByPriority(JQuickFunctionExecutor.class);
        for (JQuickFunctionExecutor executor : executors) {
            if (executor.supports(function)) {
                return executor.execute(function, input, context);
            }
        }
        throw new RuntimeException("No suitable executor found");
    }
}
