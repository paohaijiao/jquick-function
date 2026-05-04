package com.github.paohaijiao.executor.flink;

import com.github.paohaijiao.context.JQuickFunctionContext;
import com.github.paohaijiao.executor.JQuickFunctionExecutor;
import com.github.paohaijiao.function.JQuickFunction;
import com.github.paohaijiao.function.api.JQuickFlinkFunction;
import com.github.paohaijiao.spi.anno.Priority;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Priority(50)
public class JQuickFlinkFunctionExecutor implements JQuickFunctionExecutor {

    @Override
    public boolean supports(JQuickFunction<?, ?> function) {
        return function instanceof JQuickFlinkFunction;
    }

    @Override
    public <I, O> O execute(JQuickFunction<I, O> function, I input, JQuickFunctionContext context) {
        JQuickFlinkFunction<I, O> flinkFunction = (JQuickFlinkFunction<I, O>) function;
        StreamExecutionEnvironment env = context.get(StreamExecutionEnvironment.class);
        try {
            return flinkFunction.run(env, input);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;

    }

    @Override
    public String engine() {
        return "flink";
    }
}
