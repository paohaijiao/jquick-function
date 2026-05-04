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
package com.github.paohaijiao.registry;

import com.github.paohaijiao.function.JQuickIFunctionService;
import com.github.paohaijiao.spi.ServiceLoader;
import com.github.paohaijiao.spi.anno.Priority;
import com.github.paohaijiao.spi.constants.PriorityConstants;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * 基于SPI的函数注册器实现
 * 支持自动加载SPI服务，优先级排序
 */
@Priority(PriorityConstants.APPLICATION_HIGH)
public class JQuickFunctionRegistrySPIImpl implements JQuickIFunctionRegistrySPI {

    private static final long serialVersionUID = 1L;

    // 函数映射（兼容原有Function接口）
    private final Map<String, Function<Object[], Object>> functions = new ConcurrentHashMap<>();

    // 函数服务映射
    private final Map<String, JQuickIFunctionService> functionServices = new ConcurrentHashMap<>();

    // 返回类型映射
    private final Map<String, Class<?>> returnTypes = new ConcurrentHashMap<>();

    // SPI服务是否已加载
    private volatile boolean spiLoaded = false;

    public JQuickFunctionRegistrySPIImpl() {
        loadSPIServices();
    }

    /**
     * 加载SPI服务中的函数
     */
    private synchronized void loadSPIServices() {
        if (spiLoaded) {
            return;
        }
        List<JQuickIFunctionService> services = ServiceLoader.loadServicesByPriority(JQuickIFunctionService.class);
        for (JQuickIFunctionService service : services) {
            register(service);
        }
        spiLoaded = true;
    }

    @Override
    public void register(JQuickIFunctionService functionService) {
        if (functionService == null) {
            throw new IllegalArgumentException("FunctionService cannot be null");
        }
        String name = functionService.getFunctionName();
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Function name cannot be null or empty");
        }
        functionServices.put(name, functionService);
        functions.put(name, args -> functionService.execute(args));
        returnTypes.put(name, functionService.getReturnType());
    }

    @Override
    public void register(String name, Function<Object[], Object> function) {
        register(name, function, Object.class);
    }

    @Override
    public void register(String name, Function<Object[], Object> function, Class<?> returnType) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Function name cannot be null or empty");
        }
        if (function == null) {
            throw new IllegalArgumentException("Function implementation cannot be null");
        }

        functions.put(name, function);
        if (returnType != null) {
            returnTypes.put(name, returnType);
        }
    }

    @Override
    public Object call(String name, Object... args) {
        Function<Object[], Object> function = functions.get(name);
        if (function == null) {
            throw new IllegalArgumentException("Unknown function: " + name);
        }
        JQuickIFunctionService service = functionServices.get(name);
        if (service != null && !service.validateArgs(args)) {
            throw new IllegalArgumentException("Invalid arguments for function: " + name);
        }

        try {
            return function.apply(args);
        } catch (Exception e) {
            throw new RuntimeException("Error executing function: " + name, e);
        }
    }

    @Override
    public boolean contains(String name) {
        return functions.containsKey(name);
    }

    @Override
    public Optional<JQuickIFunctionService> getFunctionService(String name) {
        return Optional.ofNullable(functionServices.get(name));
    }

    @Override
    public Map<String, Function<Object[], Object>> getFunctions() {
        return Collections.unmodifiableMap(functions);
    }

    @Override
    public Map<String, JQuickIFunctionService> getFunctionServices() {
        return Collections.unmodifiableMap(functionServices);
    }

    @Override
    public Set<String> getFunctionNames() {
        return Collections.unmodifiableSet(functions.keySet());
    }

    @Override
    public boolean remove(String name) {
        functionServices.remove(name);
        returnTypes.remove(name);
        return functions.remove(name) != null;
    }

    @Override
    public void clear() {
        functions.clear();
        functionServices.clear();
        returnTypes.clear();
    }

    @Override
    public int size() {
        return functions.size();
    }

    /**
     * 重新加载SPI服务
     */
    public void reloadSPIServices() {
        spiLoaded = false;
        ServiceLoader.reload(JQuickIFunctionService.class);
        loadSPIServices();
    }

    /**
     * 获取返回类型
     */
    public Class<?> getReturnType(String name) {
        return returnTypes.get(name);
    }
}
