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
package com.github.paohaijiao.wapper;

import com.github.paohaijiao.function.JQuickFunction;

import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.logging.Logger;

/**
 * JQuickFunction 包装器，用于增强函数功能
 * <p>
 * 支持的功能：
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/5/4
 */
public class JQuickFunctionWrapper<I, O> implements JQuickFunction<I, O> {

    protected final JQuickFunction<I, O> target;

    public JQuickFunctionWrapper(JQuickFunction<I, O> target) {
        this.target = target;
    }

    /**
     * 创建带日志功能的包装器
     */
    public static <I, O> JQuickFunction<I, O> logging(JQuickFunction<I, O> function, Logger logger) {
        return new JQuickFunctionWrapper<I, O>(function) {
            @Override
            public O apply(I input) {
                logger.info(() -> String.format("Executing %s with input: %s", name(), input));
                long start = System.currentTimeMillis();
                try {
                    O result = target.apply(input);
                    long cost = System.currentTimeMillis() - start;
                    logger.info(() -> String.format("Executed %s successfully in %d ms, result: %s", name(), cost, result));
                    return result;
                } catch (Exception e) {
                    logger.severe(() -> String.format("Failed to execute %s: %s", name(), e.getMessage()));
                    throw e;
                }
            }
        };
    }

    /**
     * 创建带性能监控功能的包装器
     */
    public static <I, O> JQuickFunction<I, O> monitored(JQuickFunction<I, O> function, Consumer<Long> timeConsumer) {
        return new JQuickFunctionWrapper<I, O>(function) {
            @Override
            public O apply(I input) {
                long start = System.nanoTime();
                try {
                    return target.apply(input);
                } finally {
                    long cost = System.nanoTime() - start;
                    if (timeConsumer != null) {
                        timeConsumer.accept(cost);
                    }
                }
            }
        };
    }

    /**
     * 创建带重试功能的包装器
     */
    public static <I, O> JQuickFunction<I, O> retry(JQuickFunction<I, O> function, int maxRetries, long delayMs) {
        return new JQuickFunctionWrapper<I, O>(function) {
            @Override
            public O apply(I input) {
                Exception lastException = null;
                for (int attempt = 1; attempt <= maxRetries; attempt++) {
                    try {
                        return target.apply(input);
                    } catch (Exception e) {
                        lastException = e;
                        if (attempt < maxRetries && delayMs > 0) {
                            try {
                                Thread.sleep(delayMs);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                throw new RuntimeException("Retry interrupted", ie);
                            }
                        }
                    }
                }
                throw new RuntimeException("Failed after " + maxRetries + " retries", lastException);
            }
        };
    }

    /**
     * 创建带异常处理/降级功能的包装器
     *
     * @param function 原始函数
     * @param fallback 降级处理函数，接收异常并返回备用结果
     */
    public static <I, O> JQuickFunction<I, O> withFallback(JQuickFunction<I, O> function, Function<Exception, O> fallback) {
        return new JQuickFunctionWrapper<I, O>(function) {
            @Override
            public O apply(I input) {
                try {
                    return target.apply(input);
                } catch (Exception e) {
                    return fallback.apply(e);
                }
            }
        };
    }

    /**
     * 创建带缓存功能的包装器
     */
    public static <I, O> JQuickFunction<I, O> cached(JQuickFunction<I, O> function, ConcurrentMap<I, O> cache) {
        return new JQuickFunctionWrapper<I, O>(function) {
            @Override
            public O apply(I input) {
                return cache.computeIfAbsent(input, target::apply);
            }
        };
    }

    /**
     * 创建带前置/后置处理功能的包装器
     */
    public static <I, O> JQuickFunction<I, O> around(JQuickFunction<I, O> function, UnaryOperator<I> before, UnaryOperator<O> after) {
        return new JQuickFunctionWrapper<I, O>(function) {
            @Override
            public O apply(I input) {
                I processedInput = before != null ? before.apply(input) : input;
                O result = target.apply(processedInput);
                return after != null ? after.apply(result) : result;
            }
        };
    }

    /**
     * 创建带验证功能的包装器
     */
    public static <I, O> JQuickFunction<I, O> validated(JQuickFunction<I, O> function, Predicate<I> inputValidator, Predicate<O> outputValidator) {
        return new JQuickFunctionWrapper<I, O>(function) {
            @Override
            public O apply(I input) {
                if (inputValidator != null && !inputValidator.test(input)) {
                    throw new IllegalArgumentException("Input validation failed: " + input);
                }
                O result = target.apply(input);
                if (outputValidator != null && !outputValidator.test(result)) {
                    throw new IllegalStateException("Output validation failed: " + result);
                }
                return result;
            }
        };
    }

    /**
     * 创建带超时功能的包装器
     */
    public static <I, O> JQuickFunction<I, O> withTimeout(JQuickFunction<I, O> function, long timeoutMs, ExecutorService executor) {
        return new JQuickFunctionWrapper<I, O>(function) {
            @Override
            public O apply(I input) {
                Future<O> future = executor.submit(() -> target.apply(input));
                try {
                    return future.get(timeoutMs, TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {
                    future.cancel(true);
                    throw new RuntimeException("Function execution timed out after " + timeoutMs + " ms", e);
                } catch (Exception e) {
                    throw new RuntimeException("Function execution failed", e);
                }
            }
        };
    }

    public static <I, O> Builder<I, O> builder(JQuickFunction<I, O> function) {
        return new Builder<I, O>().withFunction(function);
    }

    @Override
    public O apply(I input) {
        return target.apply(input);
    }

    @Override
    public String name() {
        return target.name();
    }

    /**
     * 获取被包装的原始函数
     */
    public JQuickFunction<I, O> getTarget() {
        return target;
    }

    /**
     * 链式包装器构建器
     */
    public static class Builder<I, O> {

        private JQuickFunction<I, O> function;

        private Logger logger;

        private int maxRetries = 0;

        private long retryDelayMs = 0;

        private ConcurrentMap<I, O> cache;

        private Function<Exception, O> fallback;

        private Consumer<Long> timeConsumer;

        private UnaryOperator<I> before;

        private UnaryOperator<O> after;

        private Predicate<I> inputValidator;

        private Predicate<O> outputValidator;

        private Long timeoutMs;

        private ExecutorService executor;

        public Builder<I, O> withFunction(JQuickFunction<I, O> function) {
            this.function = function;
            return this;
        }

        public Builder<I, O> withLogging(Logger logger) {
            this.logger = logger;
            return this;
        }

        public Builder<I, O> withRetry(int maxRetries, long delayMs) {
            this.maxRetries = maxRetries;
            this.retryDelayMs = delayMs;
            return this;
        }

        public Builder<I, O> withCache(ConcurrentMap<I, O> cache) {
            this.cache = cache;
            return this;
        }

        public Builder<I, O> withFallback(Function<Exception, O> fallback) {
            this.fallback = fallback;
            return this;
        }

        public Builder<I, O> withMonitoring(Consumer<Long> timeConsumer) {
            this.timeConsumer = timeConsumer;
            return this;
        }

        public Builder<I, O> withBefore(UnaryOperator<I> before) {
            this.before = before;
            return this;
        }

        public Builder<I, O> withAfter(UnaryOperator<O> after) {
            this.after = after;
            return this;
        }

        public Builder<I, O> withValidation(Predicate<I> inputValidator,
                                            Predicate<O> outputValidator) {
            this.inputValidator = inputValidator;
            this.outputValidator = outputValidator;
            return this;
        }

        public Builder<I, O> withTimeout(long timeoutMs, ExecutorService executor) {
            this.timeoutMs = timeoutMs;
            this.executor = executor;
            return this;
        }

        @SuppressWarnings("unchecked")
        public JQuickFunction<I, O> build() {
            if (function == null) {
                throw new IllegalStateException("Function must not be null");
            }
            JQuickFunction<I, O> result = function;
            if (cache != null) {
                result = cached(result, cache);
            }
            if (maxRetries > 0) {
                result = retry(result, maxRetries, retryDelayMs);
            }
            if (timeConsumer != null) {
                result = monitored(result, timeConsumer);
            }
            if (logger != null) {
                result = logging(result, logger);
            }
            if (before != null || after != null) {
                result = around(result, before, after);
            }
            if (inputValidator != null || outputValidator != null) {
                result = validated(result, inputValidator, outputValidator);
            }

            return result;
        }
    }
}