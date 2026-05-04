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
package com.github.paohaijiao.function;

/**
 * 抽象函数服务基类
 */
public abstract class JQuickAbstractFunctionService implements JQuickIFunctionService {

    private static final long serialVersionUID = 1L;

    protected final String name;
    protected final String description;
    protected final Class<?> returnType;

    protected JQuickAbstractFunctionService(String name) {
        this(name, "", Object.class);
    }

    protected JQuickAbstractFunctionService(String name, String description) {
        this(name, description, Object.class);
    }

    protected JQuickAbstractFunctionService(String name, String description, Class<?> returnType) {
        this.name = name;
        this.description = description;
        this.returnType = returnType;
    }

    @Override
    public String getFunctionName() {
        return name;
    }

    @Override
    public Class<?> getReturnType() {
        return returnType;
    }

    @Override
    public String getDescription() {
        return description;
    }

    /**
     * 安全地获取参数值
     */
    protected <T> T getArg(Object[] args, int index, Class<T> type) {
        if (args == null || args.length <= index) {
            throw new IllegalArgumentException("参数索引 " + index + " 超出范围");
        }
        Object arg = args[index];
        if (arg == null) {
            throw new IllegalArgumentException("参数 " + index + " 不能为null");
        }
        if (!type.isInstance(arg)) {
            throw new IllegalArgumentException("参数 " + index + " 类型不匹配，期望: " + type.getName() + ", 实际: " + arg.getClass().getName());
        }
        return type.cast(arg);
    }

    /**
     * 获取参数数量
     */
    protected int getArgCount(Object[] args) {
        return args == null ? 0 : args.length;
    }
}
