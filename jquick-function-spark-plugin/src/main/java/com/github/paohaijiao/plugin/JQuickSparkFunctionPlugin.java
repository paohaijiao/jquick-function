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
package com.github.paohaijiao.plugin;

import com.github.paohaijiao.context.JQuickFunctionContext;
import com.github.paohaijiao.function.JQuickFunction;
import com.github.paohaijiao.spi.anno.Priority;

@Priority(50)
public class JQuickSparkFunctionPlugin implements JQuickFunctionPlugin {

    @Override
    public boolean supports(JQuickFunction<?, ?> function) {
        try {
            Class<?> sparkFuncClass = Class.forName("com.github.paohaijiao.function.api.JQuickSparkFunction");
            return sparkFuncClass.isAssignableFrom(function.getClass());
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    @Override
    public <I, O> O execute(JQuickFunction<I, O> function, I input, JQuickFunctionContext context) {
        try {
            Object sparkFunction = function;
            Object sc = context.get(Class.forName("org.apache.spark.api.java.JavaSparkContext"));
            java.lang.reflect.Method runMethod = sparkFunction.getClass().getMethod("run",
                    Class.forName("org.apache.spark.api.java.JavaSparkContext"), Object.class);

            @SuppressWarnings("unchecked")
            O result = (O) runMethod.invoke(sparkFunction, sc, input);
            return result;
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute Spark function", e);
        }
    }

    @Override
    public String engine() {
        return "spark";
    }
}
