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

/**
 * packageName com.github.paohaijiao.plugin
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/4
 */

import com.github.paohaijiao.context.JQuickFunctionContext;
import com.github.paohaijiao.function.JQuickFlinkAggregateFunction;
import com.github.paohaijiao.function.JQuickFunction;
import com.github.paohaijiao.spi.anno.Priority;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink聚合插件
 * 优先级45，介于Spark(40)和本地(10)之间
 */
@Priority(45)
public class JQuickFlinkAggregationPlugin implements JQuickFunctionPlugin {

    @Override
    public boolean supports(JQuickFunction<?, ?> function) {
        return function instanceof JQuickFlinkAggregateFunction;
    }

    @Override
    public <I, O> O execute(JQuickFunction<I, O> function, I input, JQuickFunctionContext context) {
        JQuickFlinkAggregateFunction<I, O> aggFunction = (JQuickFlinkAggregateFunction<I, O>) function;
        // 从上下文获取或创建Flink环境
        StreamExecutionEnvironment env = getOrCreateFlinkEnvironment(context);
        return aggFunction.aggregate(env, input, context);
    }

    /**
     * 获取或创建Flink环境
     */
    private StreamExecutionEnvironment getOrCreateFlinkEnvironment(JQuickFunctionContext context) {
        StreamExecutionEnvironment env = context.get(StreamExecutionEnvironment.class);
        if (env == null) {
            env = createFlinkEnvironment(context);
            context.put(StreamExecutionEnvironment.class, env);
        }
        return env;
    }

    /**
     * 创建Flink环境
     */
    private StreamExecutionEnvironment createFlinkEnvironment(JQuickFunctionContext context) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 配置并行度
        Integer parallelism = context.get(Integer.class, 5);//"flink.parallelism"
        if (parallelism != null) {
            env.setParallelism(parallelism);
        }
        // 配置检查点
        Long checkpointInterval = context.get(Long.class, 20L);//"flink.checkpoint.interval"
        if (checkpointInterval != null) {
            env.enableCheckpointing(checkpointInterval);
        }
        return env;
    }

    @Override
    public String engine() {
        return "flink";
    }
}
