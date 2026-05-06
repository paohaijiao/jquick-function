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


import com.github.paohaijiao.provider.standard.JQuickFlinkBaseStandardProvider;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Arrays;
import java.util.Collections;

/**
 * Flink ScalarFunction - 包装 StandardProvider 为 Flink UDF
 */
public class StandardProviderScalarFunction<R> extends ScalarFunction {

    private final JQuickFlinkBaseStandardProvider<R> provider;

    public StandardProviderScalarFunction(JQuickFlinkBaseStandardProvider<R> provider) {
        this.provider = provider;
    }

    /**
     * eval 方法 - 单参数
     */
    public R eval(Object arg0) {
        return provider.apply(Collections.singletonList(arg0));
    }

    /**
     * eval 方法 - 双参数
     */
    public R eval(Object arg0, Object arg1) {
        return provider.apply(Arrays.asList(arg0, arg1));
    }

    /**
     * eval 方法 - 三参数
     */
    public R eval(Object arg0, Object arg1, Object arg2) {
        return provider.apply(Arrays.asList(arg0, arg1, arg2));
    }

    /**
     * eval 方法 - 四参数
     */
    public R eval(Object arg0, Object arg1, Object arg2, Object arg3) {
        return provider.apply(Arrays.asList(arg0, arg1, arg2, arg3));
    }

    /**
     * eval 方法 - 五参数
     */
    public R eval(Object arg0, Object arg1, Object arg2, Object arg3, Object arg4) {
        return provider.apply(Arrays.asList(arg0, arg1, arg2, arg3, arg4));
    }
}