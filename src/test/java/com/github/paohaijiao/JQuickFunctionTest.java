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
package com.github.paohaijiao;

import com.github.paohaijiao.function.JQuickAbstractFunctionService;
import com.github.paohaijiao.manage.JQuickFunctionRegistryManager;
import org.junit.Test;

/**
 * packageName com.github.paohaijiao
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/4
 */
public class JQuickFunctionTest {

    @Test
    public void test(){
        JQuickFunctionRegistryManager manager = JQuickFunctionRegistryManager.getInstance();
        String lower = (String) manager.call("toLower", "HELLO WORLD");
        System.out.println(lower); // hello world

        Number sum = (Number) manager.call("sum", 1, 2, 3, 4, 5);
        System.out.println(sum); // 15.0
        manager.register("greet", args -> "Hello, " + args[0]);
        String greeting = (String) manager.call("greet", "World");
        System.out.println(greeting); // Hello, World
        manager.registerService(new JQuickAbstractFunctionService("multiply", "乘法") {
            @Override
            public Object execute(Object... args) {
                double result = 1;
                for (Object arg : args) {
                    result *= ((Number) arg).doubleValue();
                }
                return result;
            }
        });

        // 获取函数信息
        System.out.println("函数数量: " + manager.size());
        System.out.println("函数列表: " + manager.getFunctionNames());
    }
}
