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

import com.github.paohaijiao.context.JQuickFunctionContext;
import com.github.paohaijiao.plugin.JQuickFunctionPlugin;
import com.github.paohaijiao.function.JQuickFunction;
import com.github.paohaijiao.spi.JQuickServiceLoader;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * 快速函数管理器
 * 负责函数的调度执行、缓存管理和便捷调用
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/4
 */
public class JQuickFunctionManager {

    private static final Map<String, JQuickFunction<?, ?>> functionCache = new ConcurrentHashMap<>();

    private static final Map<Class<?>, JQuickFunctionPlugin> executorCache = new ConcurrentHashMap<>();

    private static final ThreadLocal<JQuickFunctionContext> defaultContext = new ThreadLocal<>();

    private static final Map<String, FunctionStats> statsMap = new ConcurrentHashMap<>();

    private static volatile List<JQuickFunctionPlugin> globalExecutors = null;

    private static ExecutorService executorService = Executors.newCachedThreadPool();

    /**
     * 私有构造函数，防止实例化
     */
    public  JQuickFunctionManager() {
        throw new UnsupportedOperationException("Utility class");
    }

    /**
     * 调度执行函数（带上下文）
     *
     * @param function 要执行的函数
     * @param input    输入参数
     * @param context  上下文
     * @param <I>      输入类型
     * @param <O>      输出类型
     * @return 执行结果
     */
    public static <I, O> O dispatch(JQuickFunction<I, O> function, I input, JQuickFunctionContext context) {
        JQuickFunctionPlugin executor = getExecutor(function);
        return executor.execute(function, input, context);
    }

    /**
     * 调度执行函数（使用默认上下文）
     *
     * @param function 要执行的函数
     * @param input    输入参数
     * @param <I>      输入类型
     * @param <O>      输出类型
     * @return 执行结果
     */
    public static <I, O> O dispatch(JQuickFunction<I, O> function, I input) {
        return dispatch(function, input, getOrCreateDefaultContext());
    }

    /**
     * 调度执行函数（无输入参数）
     *
     * @param function 要执行的函数
     * @param context  上下文
     * @param <O>      输出类型
     * @return 执行结果
     */
    public static <O> O dispatch(JQuickFunction<Void, O> function, JQuickFunctionContext context) {
        return dispatch(function, null, context);
    }


    /**
     * 调度执行函数（无输入参数，使用默认上下文）
     *
     * @param function 要执行的函数
     * @param <O>      输出类型
     * @return 执行结果
     */
    public static <O> O dispatch(JQuickFunction<Void, O> function) {
        return dispatch(function, null, getOrCreateDefaultContext());
    }

    /**
     * 异步执行函数
     *
     * @param function 要执行的函数
     * @param input    输入参数
     * @param context  上下文
     * @param <I>      输入类型
     * @param <O>      输出类型
     * @return CompletableFuture 异步结果
     */
    public static <I, O> CompletableFuture<O> dispatchAsync(JQuickFunction<I, O> function, I input, JQuickFunctionContext context) {
        return CompletableFuture.supplyAsync(() -> dispatch(function, input, context), executorService);
    }

    /**
     * 异步执行函数（使用默认上下文）
     *
     * @param function 要执行的函数
     * @param input    输入参数
     * @param <I>      输入类型
     * @param <O>      输出类型
     * @return CompletableFuture 异步结果
     */
    public static <I, O> CompletableFuture<O> dispatchAsync(JQuickFunction<I, O> function, I input) {
        return CompletableFuture.supplyAsync(() -> dispatch(function, input), executorService);
    }

    /**
     * 异步执行函数（无输入参数）
     *
     * @param function 要执行的函数
     * @param context  上下文
     * @param <O>      输出类型
     * @return CompletableFuture 异步结果
     */
    public static <O> CompletableFuture<O> dispatchAsync(JQuickFunction<Void, O> function, JQuickFunctionContext context) {
        return CompletableFuture.supplyAsync(() -> dispatch(function, context), executorService);
    }

    /**
     * 异步执行函数（无输入参数，使用默认上下文）
     *
     * @param function 要执行的函数
     * @param <O>      输出类型
     * @return CompletableFuture 异步结果
     */
    public static <O> CompletableFuture<O> dispatchAsync(JQuickFunction<Void, O> function) {
        return CompletableFuture.supplyAsync(() -> dispatch(function), executorService);
    }

    /**
     * 批量异步执行
     *
     * @param function 要执行的函数
     * @param inputs   输入参数列表
     * @param context  上下文
     * @param <I>      输入类型
     * @param <O>      输出类型
     * @return CompletableFuture 列表
     */
    public static <I, O> List<CompletableFuture<O>> dispatchBatchAsync(JQuickFunction<I, O> function, List<I> inputs, JQuickFunctionContext context) {
        List<CompletableFuture<O>> futures = new ArrayList<>();
        for (I input : inputs) {
            futures.add(CompletableFuture.supplyAsync(() -> dispatch(function, input, context), executorService));
        }
        return futures;
    }

    /**
     * 批量异步执行并等待所有完成
     *
     * @param function 要执行的函数
     * @param inputs   输入参数列表
     * @param context  上下文
     * @param <I>      输入类型
     * @param <O>      输出类型
     * @return 执行结果列表
     * @throws InterruptedException 中断异常
     * @throws ExecutionException   执行异常
     */
    public static <I, O> List<O> dispatchBatchAsyncAndWait(JQuickFunction<I, O> function, List<I> inputs, JQuickFunctionContext context) throws InterruptedException, ExecutionException {
        List<CompletableFuture<O>> futures = dispatchBatchAsync(function, inputs, context);
        List<O> results = new ArrayList<>();
        for (CompletableFuture<O> future : futures) {
            results.add(future.get());
        }
        return results;
    }

    /**
     * 注册函数（按名称缓存）
     *
     * @param name     函数名称
     * @param function 函数实例
     * @param <I>      输入类型
     * @param <O>      输出类型
     */
    public static <I, O> void registerFunction(String name, JQuickFunction<I, O> function) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Function name cannot be null or empty");
        }
        functionCache.put(name, function);
    }

    /**
     * 获取已注册的函数
     *
     * @param name 函数名称
     * @param <I>  输入类型
     * @param <O>  输出类型
     * @return 函数实例
     * @throws IllegalArgumentException 如果函数未注册
     */
    @SuppressWarnings("unchecked")
    public static <I, O> JQuickFunction<I, O> getFunction(String name) {
        JQuickFunction<I, O> function = (JQuickFunction<I, O>) functionCache.get(name);
        if (function == null) {
            throw new IllegalArgumentException("Function not found: " + name);
        }
        return function;
    }

    /**
     * 通过名称调度执行函数
     *
     * @param name    函数名称
     * @param input   输入参数
     * @param context 上下文
     * @param <I>     输入类型
     * @param <O>     输出类型
     * @return 执行结果
     */
    public static <I, O> O dispatchByName(String name, I input, JQuickFunctionContext context) {
        JQuickFunction<I, O> function = getFunction(name);
        return dispatch(function, input, context);
    }

    /**
     * 通过名称调度执行函数（使用默认上下文）
     *
     * @param name  函数名称
     * @param input 输入参数
     * @param <I>   输入类型
     * @param <O>   输出类型
     * @return 执行结果
     */
    public static <I, O> O dispatchByName(String name, I input) {
        return dispatchByName(name, input, getOrCreateDefaultContext());
    }

    /**
     * 检查函数是否已注册
     *
     * @param name 函数名称
     * @return true-已注册, false-未注册
     */
    public static boolean isFunctionRegistered(String name) {
        return functionCache.containsKey(name);
    }

    /**
     * 取消注册函数
     *
     * @param name 函数名称
     * @return 被移除的函数实例
     */
    public static JQuickFunction<?, ?> unregisterFunction(String name) {
        return functionCache.remove(name);
    }

    /**
     * 获取所有已注册的函数名称
     *
     * @return 函数名称集合
     */
    public static Set<String> getRegisteredFunctionNames() {
        return new HashSet<>(functionCache.keySet());
    }

    /**
     * 清空函数缓存
     */
    public static void clearFunctionCache() {
        functionCache.clear();
        executorCache.clear();
        globalExecutors = null;
    }

    /**
     * 获取函数的执行器（带缓存）
     *
     * @param function 要执行的函数
     * @return 合适的执行器
     * @throws RuntimeException 如果没有找到合适的执行器
     */
    private static JQuickFunctionPlugin getExecutor(JQuickFunction<?, ?> function) {
        Class<?> functionClass = function.getClass();
        if (executorCache.containsKey(functionClass)) { // 先从缓存中获取
            return executorCache.get(functionClass);
        }
        List<JQuickFunctionPlugin> executors = getGlobalExecutors(); // 获取所有执行器
        for (JQuickFunctionPlugin executor : executors) {// 查找支持该函数的执行器
            if (executor.supports(function)) {
                executorCache.put(functionClass, executor);
                return executor;
            }
        }

        throw new RuntimeException("No suitable executor found for function: " + functionClass.getName());
    }

    /**
     * 获取全局执行器列表（懒加载）
     *
     * @return 执行器列表
     */
    private static List<JQuickFunctionPlugin> getGlobalExecutors() {
        if (globalExecutors == null) {
            synchronized (JQuickFunctionManager.class) {
                if (globalExecutors == null) {
                    globalExecutors = JQuickServiceLoader.loadServicesByPriority(JQuickFunctionPlugin.class);
                }
            }
        }
        return globalExecutors;
    }

    /**
     * 刷新执行器缓存（当有新的执行器注册时调用）
     */
    public static void refreshExecutors() {
        globalExecutors = null;
        executorCache.clear();
    }

    /**
     * 手动注册执行器
     *
     * @param executor 执行器实例
     */
    public static void registerExecutor(JQuickFunctionPlugin executor) {
        refreshExecutors(); // 强制刷新全局执行器列表
    }

    /**
     * 设置自定义线程池
     *
     * @param executorService 自定义线程池
     */
    public static void setExecutorService(ExecutorService executorService) {
        if (executorService != null) {
            JQuickFunctionManager.executorService = executorService;
        }
    }

    /**
     * 关闭线程池
     */
    public static void shutdown() {
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
        }
    }

    /**
     * 获取或创建默认上下文
     *
     * @return 默认上下文
     */
    private static JQuickFunctionContext getOrCreateDefaultContext() {
        JQuickFunctionContext context = defaultContext.get();
        if (context == null) {
            context = new JQuickFunctionContext();
            defaultContext.set(context);
        }
        return context;
    }

    /**
     * 获取当前线程的默认上下文
     *
     * @return 默认上下文，可能为null
     */
    public static JQuickFunctionContext getDefaultContext() {
        return defaultContext.get();
    }

    /**
     * 设置当前线程的默认上下文
     *
     * @param context 上下文实例
     */
    public static void setDefaultContext(JQuickFunctionContext context) {
        defaultContext.set(context);
    }

    /**
     * 清除当前线程的默认上下文
     */
    public static void clearDefaultContext() {
        defaultContext.remove();
    }

    /**
     * 使用指定上下文执行任务
     *
     * @param context 上下文
     * @param task    要执行的任务
     * @param <R>     返回值类型
     * @return 任务执行结果
     */
    public static <R> R withContext(JQuickFunctionContext context, Callable<R> task) {
        JQuickFunctionContext previous = getDefaultContext();
        try {
            setDefaultContext(context);
            return task.call();
        } catch (Exception e) {
            throw new RuntimeException("Error executing task with context", e);
        } finally {
            setDefaultContext(previous);
        }
    }

    /**
     * 使用指定上下文执行任务（无返回值）
     *
     * @param context 上下文
     * @param task    要执行的任务
     */
    public static void withContext(JQuickFunctionContext context, Runnable task) {
        JQuickFunctionContext previous = getDefaultContext();
        try {
            setDefaultContext(context);
            task.run();
        } finally {
            setDefaultContext(previous);
        }
    }

    /**
     * 创建函数调用构建器
     *
     * @param function 要执行的函数
     * @param <I>      输入类型
     * @param <O>      输出类型
     * @return 调用构建器
     */
    public static <I, O> FunctionCallBuilder<I, O> call(JQuickFunction<I, O> function) {
        return new FunctionCallBuilder<>(function);
    }

    /**
     * 创建函数调用构建器（按名称）
     *
     * @param name 函数名称
     * @param <I>  输入类型
     * @param <O>  输出类型
     * @return 调用构建器
     */
    public static <I, O> FunctionCallBuilder<I, O> call(String name) {
        return new FunctionCallBuilder<>(getFunction(name));
    }

    /**
     * 带重试的调度执行
     *
     * @param function   函数
     * @param input      输入
     * @param context    上下文
     * @param maxRetries 最大重试次数
     * @param <I>        输入类型
     * @param <O>        输出类型
     * @return 执行结果
     * @throws RuntimeException 重试失败后抛出
     */
    public static <I, O> O dispatchWithRetry(JQuickFunction<I, O> function, I input, JQuickFunctionContext context, int maxRetries) {
        Exception lastException = null;
        for (int i = 0; i <= maxRetries; i++) {
            try {
                return dispatch(function, input, context);
            } catch (Exception e) {
                lastException = e;
                if (i < maxRetries) {
                    try {
                        Thread.sleep(100L * (i + 1)); // 指数退避
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Retry interrupted", ie);
                    }
                }
            }
        }
        throw new RuntimeException("Failed after " + maxRetries + " retries", lastException);
    }

    /**
     * 带重试的调度执行（使用默认上下文）
     *
     * @param function   函数
     * @param input      输入
     * @param maxRetries 最大重试次数
     * @param <I>        输入类型
     * @param <O>        输出类型
     * @return 执行结果
     */
    public static <I, O> O dispatchWithRetry(JQuickFunction<I, O> function, I input, int maxRetries) {
        return dispatchWithRetry(function, input, getOrCreateDefaultContext(), maxRetries);
    }

    /**
     * 带重试的调度执行（带重试条件判断）
     *
     * @param function       函数
     * @param input          输入
     * @param context        上下文
     * @param maxRetries     最大重试次数
     * @param retryPredicate 重试条件判断
     * @param <I>            输入类型
     * @param <O>            输出类型
     * @return 执行结果
     */
    public static <I, O> O dispatchWithRetry(JQuickFunction<I, O> function, I input, JQuickFunctionContext context, int maxRetries, Function<Exception, Boolean> retryPredicate) {
        Exception lastException = null;
        for (int i = 0; i <= maxRetries; i++) {
            try {
                return dispatch(function, input, context);
            } catch (Exception e) {
                lastException = e;
                if (i < maxRetries && retryPredicate.apply(e)) {
                    try {
                        Thread.sleep(100L * (i + 1));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Retry interrupted", ie);
                    }
                } else if (i < maxRetries) {
                    break;
                }
            }
        }
        throw new RuntimeException("Failed after " + maxRetries + " retries", lastException);
    }

    /**
     * 带超时的调度执行
     *
     * @param function      函数
     * @param input         输入
     * @param context       上下文
     * @param timeoutMillis 超时时间（毫秒）
     * @param <I>           输入类型
     * @param <O>           输出类型
     * @return 执行结果
     * @throws TimeoutException 超时异常
     */
    public static <I, O> O dispatchWithTimeout(JQuickFunction<I, O> function, I input,
                                               JQuickFunctionContext context, long timeoutMillis)
            throws TimeoutException {
        CompletableFuture<O> future = dispatchAsync(function, input, context);
        try {
            return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Execution interrupted", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Execution failed", e.getCause());
        }
    }

    /**
     * 启用统计的调度执行
     *
     * @param function     函数
     * @param input        输入
     * @param context      上下文
     * @param functionName 函数名称（用于统计）
     * @param <I>          输入类型
     * @param <O>          输出类型
     * @return 执行结果
     */
    public static <I, O> O dispatchWithStats(JQuickFunction<I, O> function, I input, JQuickFunctionContext context, String functionName) {
        long startTime = System.nanoTime();
        try {
            return dispatch(function, input, context);
        } finally {
            long duration = System.nanoTime() - startTime;
            FunctionStats stats = statsMap.computeIfAbsent(functionName, k -> new FunctionStats());
            stats.record(duration);
        }
    }

    /**
     * 获取函数统计信息
     *
     * @param functionName 函数名称
     * @return 统计信息
     */
    public static FunctionStats getFunctionStats(String functionName) {
        return statsMap.get(functionName);
    }

    /**
     * 获取所有函数统计信息
     *
     * @return 统计信息映射
     */
    public static Map<String, FunctionStats> getAllStats() {
        return new HashMap<>(statsMap);
    }

    /**
     * 清除统计信息
     */
    public static void clearStats() {
        statsMap.clear();
    }

    /**
     * 判断函数是否可执行
     *
     * @param function 函数
     * @return true-可执行, false-不可执行
     */
    public static boolean isExecutable(JQuickFunction<?, ?> function) {
        try {
            getExecutor(function);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    /**
     * 获取支持的函数类型列表
     *
     * @return 支持的类型映射
     */
    public static Map<Class<?>, JQuickFunctionPlugin> getSupportedFunctions() {
        return new HashMap<>(executorCache);
    }

    /**
     * 函数调用构建器
     */
    public static class FunctionCallBuilder<I, O> {
        private final JQuickFunction<I, O> function;
        private I input;
        private JQuickFunctionContext context;

        private FunctionCallBuilder(JQuickFunction<I, O> function) {
            this.function = function;
        }

        /**
         * 设置输入参数
         */
        public FunctionCallBuilder<I, O> withInput(I input) {
            this.input = input;
            return this;
        }

        /**
         * 设置上下文
         */
        public FunctionCallBuilder<I, O> withContext(JQuickFunctionContext context) {
            this.context = context;
            return this;
        }

        /**
         * 同步执行
         */
        public O execute() {
            JQuickFunctionContext execContext = context != null ? context : getOrCreateDefaultContext();
            return dispatch(function, input, execContext);
        }

        /**
         * 异步执行
         */
        public CompletableFuture<O> executeAsync() {
            JQuickFunctionContext execContext = context != null ? context : getOrCreateDefaultContext();
            return dispatchAsync(function, input, execContext);
        }
    }

    /**
     * 函数统计信息类
     */
    public static class FunctionStats {

        private final AtomicLong callCount = new AtomicLong(0);

        private final AtomicLong totalTime = new AtomicLong(0);

        private final AtomicLong maxTime = new AtomicLong(0);

        public void record(long duration) {
            callCount.incrementAndGet();
            totalTime.addAndGet(duration);
            maxTime.updateAndGet(current -> Math.max(current, duration));
        }

        public long getCallCount() {
            return callCount.get();
        }

        public long getTotalTime() {
            return totalTime.get();
        }

        public long getMaxTime() {
            return maxTime.get();
        }

        public double getAvgTime() {
            long count = callCount.get();
            return count > 0 ? (double) totalTime.get() / count : 0;
        }

        @Override
        public String toString() {
            return String.format("FunctionStats{callCount=%d, totalTime=%d, avgTime=%.2f, maxTime=%d}", callCount.get(), totalTime.get(), getAvgTime(), maxTime.get());
        }
    }
}