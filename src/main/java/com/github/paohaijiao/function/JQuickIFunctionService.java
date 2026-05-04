package com.github.paohaijiao.function;

import java.io.Serializable;

public interface JQuickIFunctionService extends Serializable {
    /**
     * 获取函数名称
     */
    String getFunctionName();

    /**
     * 执行函数
     * @param args 参数数组
     * @return 执行结果
     */
    Object execute(Object... args);

    /**
     * 获取返回类型
     */
    default Class<?> getReturnType() {
        return Object.class;
    }

    /**
     * 验证参数
     * @param args 参数数组
     * @return 是否有效
     */
    default boolean validateArgs(Object... args) {
        return true;
    }

    /**
     * 获取函数描述（可选）
     */
    default String getDescription() {
        return "";
    }
}
