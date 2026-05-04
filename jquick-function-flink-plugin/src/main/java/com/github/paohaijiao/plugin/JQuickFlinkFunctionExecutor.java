package com.github.paohaijiao.plugin;

import com.github.paohaijiao.context.JQuickFunctionContext;
import com.github.paohaijiao.function.JQuickFlinkFunction;
import com.github.paohaijiao.function.JQuickFunction;
import com.github.paohaijiao.spi.anno.Priority;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Priority(50)
public class JQuickFlinkFunctionExecutor implements JQuickFunctionPlugin{

    @Override
    public boolean supports(JQuickFunction<?, ?> function) {
        return isFlinkFunction(function);
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
    private boolean isFlinkFunction(JQuickFunction<?, ?> function) {
        try {
            Class<?> flinkFuncClass = Class.forName("com.github.paohaijiao.function.JQuickFlinkFunction");
            return flinkFuncClass.isAssignableFrom(function.getClass());
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
}
