package com.github.paohaijiao.registry;


import com.github.paohaijiao.function.JQuickIFunctionService;
import com.github.paohaijiao.spi.anno.Priority;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * 函数注册器SPI接口
 * 支持优先级排序的插件化函数管理
 */
@Priority(100) // 应用级别默认优先级
public interface JQuickIFunctionRegistrySPI {

    /**
     * 注册单个函数
     */
    void register(JQuickIFunctionService functionService);

    /**
     * 批量注册函数
     */
    default void registerAll(JQuickIFunctionService... services) {
        for (JQuickIFunctionService service : services) {
            register(service);
        }
    }

    /**
     * 注册函数（兼容原有Function接口）
     */
    void register(String name, Function<Object[], Object> function);

    /**
     * 注册带类型的函数
     */
    void register(String name, Function<Object[], Object> function, Class<?> returnType);

    /**
     * 调用函数
     */
    Object call(String name, Object... args);

    /**
     * 检查函数是否存在
     */
    boolean contains(String name);

    /**
     * 获取函数服务
     */
    Optional<JQuickIFunctionService> getFunctionService(String name);

    /**
     * 获取所有注册的函数
     */
    Map<String, Function<Object[], Object>> getFunctions();

    /**
     * 获取所有函数服务
     */
    Map<String, JQuickIFunctionService> getFunctionServices();

    /**
     * 获取所有函数名称
     */
    Set<String> getFunctionNames();

    /**
     * 移除函数
     */
    boolean remove(String name);

    /**
     * 清空所有函数
     */
    void clear();

    /**
     * 获取函数数量
     */
    int size();
}
