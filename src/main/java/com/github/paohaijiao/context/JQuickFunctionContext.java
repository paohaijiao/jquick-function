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
package com.github.paohaijiao.context;

import java.util.HashMap;
import java.util.Map;

/**
 * packageName com.github.paohaijiao.context
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/4
 */
public class JQuickFunctionContext {
    private final Map<Class<?>, Object> params = new HashMap<>();

    public <T> void put(Class<T> key, T value) {
        params.put(key, value);
    }

    public <T> T get(Class<T> key) {
        return (T) params.get(key);
    }
}
