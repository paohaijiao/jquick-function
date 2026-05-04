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
package com.github.paohaijiao.manage;
import com.github.paohaijiao.function.JQuickIFunctionService;
import com.github.paohaijiao.registry.JQuickFunctionRegistry;
import com.github.paohaijiao.registry.JQuickIFunctionRegistrySPI;
import com.github.paohaijiao.spi.ServiceLoader;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * 函数注册器管理器
 * 提供统一的函数注册和调用入口，基于SPI实现
 */
public class JQuickFunctionRegistryManager implements JQuickFunctionRegistry {

    private static final long serialVersionUID = 1L;

    private static volatile JQuickFunctionRegistryManager instance;

    private final JQuickIFunctionRegistrySPI registry;

    private JQuickFunctionRegistryManager() {
        // 通过SPI获取最高优先级的注册器实现
        Optional<JQuickIFunctionRegistrySPI> optional = ServiceLoader.getHighestPriorityService(JQuickIFunctionRegistrySPI.class);
        this.registry = optional.orElseThrow(() -> new RuntimeException("未找到 FunctionRegistrySPI 实现"));
    }

    /**
     * 获取单例实例
     */
    public static JQuickFunctionRegistryManager getInstance() {
        if (instance == null) {
            synchronized (JQuickFunctionRegistryManager.class) {
                if (instance == null) {
                    instance = new JQuickFunctionRegistryManager();
                }
            }
        }
        return instance;
    }

    /**
     * 创建新的注册器实例
     */
    public static JQuickFunctionRegistryManager create() {
        return new JQuickFunctionRegistryManager();
    }

    @Override
    public void register(String name, Function<Object[], Object> function) {
        registry.register(name, function);
    }

    @Override
    public void register(String name, Function<Object[], Object> function, Class<?> returnType) {
        registry.register(name, function, returnType);
    }

    /**
     * 注册函数服务
     */
    public void registerService(JQuickIFunctionService functionService) {
        registry.register(functionService);
    }

    @Override
    public Object call(String name, Object... args) {
        return registry.call(name, args);
    }

    @Override
    public boolean contains(String name) {
        return registry.contains(name);
    }

    @Override
    public Map<String, Function<Object[], Object>> getFunctions() {
        return registry.getFunctions();
    }

    /**
     * 获取函数服务
     */
    public Optional<JQuickIFunctionService> getFunctionService(String name) {
        return registry.getFunctionService(name);
    }

    /**
     * 获取所有函数服务
     */
    public Map<String, JQuickIFunctionService> getFunctionServices() {
        return registry.getFunctionServices();
    }

    /**
     * 获取函数名称列表
     */
    public java.util.Set<String> getFunctionNames() {
        return registry.getFunctionNames();
    }

    /**
     * 移除函数
     */
    public boolean remove(String name) {
        return registry.remove(name);
    }

    /**
     * 清空所有函数
     */
    public void clear() {
        registry.clear();
    }

    /**
     * 获取函数数量
     */
    public int size() {
        return registry.size();
    }
}