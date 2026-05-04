package com.github.paohaijiao.registry;
import java.io.Serializable;
import java.util.Map;
import java.util.function.Function;

/**
 * 函数注册器接口
 */
public interface JQuickFunctionRegistry extends Serializable {

    /**
     * 注册函数
     * @param name 函数名称
     * @param function 函数实现
     */
    void register(String name, Function<Object[], Object> function);

    /**
     * 注册带类型的函数
     * @param name 函数名称
     * @param function 函数实现
     * @param returnType 返回类型
     */
    void register(String name, Function<Object[], Object> function, Class<?> returnType);

    /**
     * 调用函数
     * @param name 函数名称
     * @param args 参数
     * @return 执行结果
     */
    Object call(String name, Object... args);

    /**
     * 检查函数是否存在
     */
    boolean contains(String name);

    /**
     * 获取所有注册的函数
     */
    Map<String, Function<Object[], Object>> getFunctions();
}
