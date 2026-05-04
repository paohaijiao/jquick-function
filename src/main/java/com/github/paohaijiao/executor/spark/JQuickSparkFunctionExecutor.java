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
package com.github.paohaijiao.executor.spark;

import com.github.paohaijiao.context.JQuickFunctionContext;
import com.github.paohaijiao.executor.JQuickFunctionExecutor;
import com.github.paohaijiao.function.JQuickFunction;
import com.github.paohaijiao.function.api.JQuickSparkFunction;
import com.github.paohaijiao.spi.anno.Priority;
import org.apache.spark.api.java.JavaSparkContext;

@Priority(50)
public class JQuickSparkFunctionExecutor implements JQuickFunctionExecutor {

    @Override
    public boolean supports(JQuickFunction<?, ?> function) {
        return function instanceof JQuickSparkFunction;
    }

    @Override
    public <I, O> O execute(JQuickFunction<I, O> function, I input, JQuickFunctionContext context) {
        JQuickSparkFunction<I, O> sparkFunction = (JQuickSparkFunction<I, O>) function;
        JavaSparkContext sc = context.get(JavaSparkContext.class);
        return sparkFunction.run(sc, input);
    }

    @Override
    public String engine() {
        return "spark";
    }
}
