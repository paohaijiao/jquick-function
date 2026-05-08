///*
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// *
// * Copyright (c) [2025-2099] Martin (goudingcheng@gmail.com)
// */
//package com.github.paohaijiao.manage;
//
//import com.github.paohaijiao.compute.JQuickComputeTypeImpl;
//import com.github.paohaijiao.core.constant.JQuickProviderTypeConstants;
//import com.github.paohaijiao.provider.JQuickFunctionProvider;
//import com.github.paohaijiao.spi.ServiceLoader;
//import com.github.paohaijiao.spi.anno.Priority;
//import com.github.paohaijiao.spi.constants.PriorityConstants;
//import com.github.paohaijiao.type.JQuickProviderType;
//import com.github.paohaijiao.type.impl.JQuickFlinkProvider;
//import com.github.paohaijiao.type.impl.JQuickJavaProvider;
//import com.github.paohaijiao.type.impl.JQuickSparkProvider;
//
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.function.Predicate;
//import java.util.stream.Collectors;
//
///**
// * JQuickFunctionProvider 管理器
// * 负责加载和管理 Java、Spark、Flink 三种计算方法提供者
// *
// * @author Martin
// * @version 1.0.0
// * @since 2026/5/5
// */
//public class JQuickFunctionProviderManager {
//
//    public static final String COMPUTE_TYPE_JAVA = JQuickProviderTypeConstants.JAVA;
//
//    public static final String COMPUTE_TYPE_SPARK = JQuickProviderTypeConstants.SPARK;
//
//    public static final String COMPUTE_TYPE_FLINK = JQuickProviderTypeConstants.Flink;
//
//    private static final Map<String, JQuickProviderType> PROVIDER_TYPE_MAP = new ConcurrentHashMap<>();
//
//    private static volatile JQuickFunctionProviderManager instance;
//
//    static {
//        PROVIDER_TYPE_MAP.put(COMPUTE_TYPE_JAVA, new JQuickJavaProvider());
//        PROVIDER_TYPE_MAP.put(COMPUTE_TYPE_SPARK, new JQuickSparkProvider());
//        PROVIDER_TYPE_MAP.put(COMPUTE_TYPE_FLINK, new JQuickFlinkProvider());
//    }
//
//    private final Map<String, List<JQuickFunctionProvider>> functionProviderCache = new ConcurrentHashMap<>();
//
//    private final Map<String, Map<String, JQuickFunctionProvider<?, ?>>> methodCache = new ConcurrentHashMap<>();
//
//    private JQuickFunctionProviderManager() {
//        methodCache.put(COMPUTE_TYPE_JAVA, new ConcurrentHashMap<>());
//        methodCache.put(COMPUTE_TYPE_SPARK, new ConcurrentHashMap<>());
//        methodCache.put(COMPUTE_TYPE_FLINK, new ConcurrentHashMap<>());
//    }
//
//    /**
//     * 获取管理器单例实例
//     */
//    public static JQuickFunctionProviderManager getInstance() {
//        if (instance == null) {
//            synchronized (JQuickFunctionProviderManager.class) {
//                if (instance == null) {
//                    instance = new JQuickFunctionProviderManager();
//                }
//            }
//        }
//        return instance;
//    }
//
//    /**
//     * 加载所有可用的函数提供者
//     *
//     * @return 所有函数提供者列表（按优先级排序）
//     */
//    @SuppressWarnings("rawtypes")
//    public List<JQuickFunctionProvider> loadAllProviders() {
//        return ServiceLoader.loadServicesByPriority(JQuickFunctionProvider.class);
//    }
//
//    /**
//     * 按计算类型加载函数提供者
//     *
//     * @param computeType 计算类型 (JAVA/SPARK/FLINK)
//     * @return 对应类型的函数提供者列表
//     */
//    @SuppressWarnings({"rawtypes", "unchecked"})
//    public List<JQuickFunctionProvider> loadProvidersByComputeType(String computeType) {
//        return functionProviderCache.computeIfAbsent(computeType, key -> {
//            List<JQuickFunctionProvider> allProviders = loadAllProviders();
//            return allProviders.stream()
//                    .filter(provider -> matchesComputeType(provider, computeType))
//                    .collect(Collectors.toList());
//        });
//    }
//
//    /**
//     * 根据计算类型和优先级范围加载函数提供者
//     *
//     * @param computeType 计算类型
//     * @param minPriority 最小优先级
//     * @param maxPriority 最大优先级
//     * @return 符合条件的函数提供者列表
//     */
//    public List<JQuickFunctionProvider> loadProvidersByComputeTypeAndPriority(String computeType, int minPriority, int maxPriority) {
//        return loadProvidersByComputeType(computeType).stream()
//                .filter(provider -> {
//                    int priority = getProviderPriority(provider);
//                    return priority >= minPriority && priority <= maxPriority;
//                })
//                .collect(Collectors.toList());
//    }
//
//    /**
//     * 获取最高优先级的函数提供者（按计算类型）
//     *
//     * @param computeType 计算类型
//     * @return 最高优先级的函数提供者
//     */
//    public Optional<JQuickFunctionProvider> getHighestPriorityProvider(String computeType) {
//        List<JQuickFunctionProvider> providers = loadProvidersByComputeType(computeType);
//        return providers.isEmpty() ? Optional.empty() : Optional.of(providers.get(0));
//    }
//
//    /**
//     * 获取指定计算类型的系统级提供者
//     */
//    public List<JQuickFunctionProvider> getSystemLevelProviders(String computeType) {
//        return loadProvidersByComputeTypeAndPriority(computeType,
//                PriorityConstants.SYSTEM_HIGHEST, PriorityConstants.SYSTEM_LOW);
//    }
//
//    /**
//     * 获取指定计算类型的应用级提供者
//     */
//    public List<JQuickFunctionProvider> getApplicationLevelProviders(String computeType) {
//        return loadProvidersByComputeTypeAndPriority(computeType, PriorityConstants.APPLICATION_HIGHEST, PriorityConstants.APPLICATION_LOW);
//    }
//
//    /**
//     * 获取指定计算类型的业务级提供者
//     */
//    public List<JQuickFunctionProvider> getBusinessLevelProviders(String computeType) {
//        return loadProvidersByComputeTypeAndPriority(computeType, PriorityConstants.BUSINESS_HIGHEST, PriorityConstants.BUSINESS_LOW);
//    }
//
//    /**
//     * 获取指定计算类型的用户级提供者
//     */
//    public List<JQuickFunctionProvider> getUserLevelProviders(String computeType) {
//        return loadProvidersByComputeTypeAndPriority(computeType, PriorityConstants.USER_HIGHEST, PriorityConstants.USER_LOW);
//    }
//
//    /**
//     * 按名称查找函数提供者
//     *
//     * @param providerName 提供者类名或名称
//     * @return 匹配的函数提供者
//     */
//    public Optional<JQuickFunctionProvider> findProviderByName(String providerName) {
//        return loadAllProviders().stream()
//                .filter(provider -> provider.getClass().getSimpleName().equals(providerName)
//                        || provider.getClass().getName().equals(providerName))
//                .findFirst();
//    }
//
//    /**
//     * 按条件筛选函数提供者
//     *
//     * @param condition 筛选条件
//     * @return 符合条件的函数提供者列表
//     */
//    public List<JQuickFunctionProvider> filterProviders(Predicate<JQuickFunctionProvider> condition) {
//        return loadAllProviders().stream().filter(condition).collect(Collectors.toList());
//    }
//
//    /**
//     * 获取所有 Java 计算提供者
//     */
//    public List<JQuickFunctionProvider> getJavaProviders() {
//        return loadProvidersByComputeType(COMPUTE_TYPE_JAVA);
//    }
//
//    /**
//     * 获取所有 Spark 计算提供者
//     */
//    public List<JQuickFunctionProvider> getSparkProviders() {
//        return loadProvidersByComputeType(COMPUTE_TYPE_SPARK);
//    }
//
//    /**
//     * 获取所有 Flink 计算提供者
//     */
//    public List<JQuickFunctionProvider> getFlinkProviders() {
//        return loadProvidersByComputeType(COMPUTE_TYPE_FLINK);
//    }
//
//
//
//    /**
//     * 执行函数 - 使用指定类型的最高优先级提供者
//     *
//     * @param computeType 计算类型
//     * @param args        参数列表
//     * @return 执行结果
//     * @throws NoSuchElementException 如果没有找到对应类型的提供者
//     */
//    public Object execute(String computeType, List<?> args) {
//        JQuickFunctionProvider provider = getHighestPriorityProvider(computeType)
//                .orElseThrow(() -> new NoSuchElementException(
//                        "No function provider found for compute type: " + computeType));
//        return provider.apply(args);
//    }
//
//    /**
//     * 执行函数 - 单参数版本
//     */
//    public Object executeOne(String computeType, Object arg) {
//        JQuickFunctionProvider provider = getHighestPriorityProvider(computeType)
//                .orElseThrow(() -> new NoSuchElementException(
//                        "No function provider found for compute type: " + computeType));
//        return provider.applyOne(arg);
//    }
//
//    /**
//     * 执行函数 - 双参数版本
//     */
//    public Object executeTwo(String computeType, Object arg1, Object arg2) {
//        JQuickFunctionProvider provider = getHighestPriorityProvider(computeType)
//                .orElseThrow(() -> new NoSuchElementException(
//                        "No function provider found for compute type: " + computeType));
//        return provider.applyTwo(arg1, arg2);
//    }
//
//    /**
//     * 执行函数 - 三参数版本
//     */
//    public Object executeThree(String computeType, Object arg1, Object arg2, Object arg3) {
//        JQuickFunctionProvider provider = getHighestPriorityProvider(computeType)
//                .orElseThrow(() -> new NoSuchElementException(
//                        "No function provider found for compute type: " + computeType));
//        return provider.applyThree(arg1, arg2, arg3);
//    }
//
//    /**
//     * 使用指定提供者执行函数
//     *
//     * @param provider 函数提供者
//     * @param args     参数列表
//     * @return 执行结果
//     */
//    public Object executeWithProvider(JQuickFunctionProvider provider, List<?> args) {
//        if (provider == null) {
//            throw new IllegalArgumentException("Provider cannot be null");
//        }
//        return provider.apply(args);
//    }
//    /**
//     * 根据方法名执行 Java 计算
//     *
//     * @param methodName 方法名称
//     * @param args       参数列表
//     * @return 执行结果
//     * @throws NoSuchElementException 如果没有找到对应方法名的提供者
//     */
//    public Object executeJavaByMethod(String methodName, List<?> args) {
//        JQuickFunctionProvider provider = getJavaProviderByMethod(methodName)
//                .orElseThrow(() -> new NoSuchElementException(
//                        "No Java function provider found for method: " + methodName));
//        return provider.apply(args);
//    }
//
//    /**
//     * 根据方法名执行 Java 计算 - 单参数版本
//     */
//    public Object executeJavaByMethodOne(String methodName, Object arg) {
//        JQuickFunctionProvider provider = getJavaProviderByMethod(methodName)
//                .orElseThrow(() -> new NoSuchElementException("No Java function provider found for method: " + methodName));
//        return provider.applyOne(arg);
//    }
//
//    /**
//     * 根据方法名执行 Java 计算 - 双参数版本
//     */
//    public Object executeJavaByMethodTwo(String methodName, Object arg1, Object arg2) {
//        JQuickFunctionProvider provider = getJavaProviderByMethod(methodName)
//                .orElseThrow(() -> new NoSuchElementException("No Java function provider found for method: " + methodName));
//        return provider.applyTwo(arg1, arg2);
//    }
//
//    /**
//     * 根据方法名执行 Java 计算 - 三参数版本
//     */
//    public Object executeJavaByMethodThree(String methodName, Object arg1, Object arg2, Object arg3) {
//        JQuickFunctionProvider provider = getJavaProviderByMethod(methodName)
//                .orElseThrow(() -> new NoSuchElementException("No Java function provider found for method: " + methodName));
//        return provider.applyThree(arg1, arg2, arg3);
//    }
//
//    /**
//     * 根据方法名获取 Java 函数提供者
//     *
//     * @param methodName 方法名称
//     * @return 匹配的 Java 函数提供者
//     */
//    public Optional<JQuickFunctionProvider> getJavaProviderByMethod(String methodName) {
//        return getProviderByMethod(COMPUTE_TYPE_JAVA, methodName);
//    }
//
//    /**
//     * 获取所有可用的 Java 方法名称列表
//     */
//    public List<String> getAvailableJavaMethodNames() {
//        return getAvailableMethodNames(COMPUTE_TYPE_JAVA);
//    }
//    /**
//     * 根据方法名执行 Spark 计算
//     *
//     * @param methodName 方法名称（如：sum、avg、count、max、min等）
//     * @param args       参数列表
//     * @return 执行结果
//     * @throws NoSuchElementException 如果没有找到对应方法名的提供者
//     */
//    public Object executeSparkByMethod(String methodName, List<?> args) {
//        JQuickFunctionProvider provider = getSparkProviderByMethod(methodName)
//                .orElseThrow(() -> new NoSuchElementException("No Spark function provider found for method: " + methodName));
//        return provider.apply(args);
//    }
//
//    /**
//     * 根据方法名执行 Spark 计算 - 单参数版本
//     */
//    public Object executeSparkByMethodOne(String methodName, Object arg) {
//        JQuickFunctionProvider provider = getSparkProviderByMethod(methodName)
//                .orElseThrow(() -> new NoSuchElementException("No Spark function provider found for method: " + methodName));
//        return provider.applyOne(arg);
//    }
//
//    /**
//     * 根据方法名执行 Spark 计算 - 双参数版本
//     */
//    public Object executeSparkByMethodTwo(String methodName, Object arg1, Object arg2) {
//        JQuickFunctionProvider provider = getSparkProviderByMethod(methodName)
//                .orElseThrow(() -> new NoSuchElementException("No Spark function provider found for method: " + methodName));
//        return provider.applyTwo(arg1, arg2);
//    }
//
//    /**
//     * 根据方法名执行 Spark 计算 - 三参数版本
//     */
//    public Object executeSparkByMethodThree(String methodName, Object arg1, Object arg2, Object arg3) {
//        JQuickFunctionProvider provider = getSparkProviderByMethod(methodName)
//                .orElseThrow(() -> new NoSuchElementException("No Spark function provider found for method: " + methodName));
//        return provider.applyThree(arg1, arg2, arg3);
//    }
//
//    /**
//     * 根据方法名获取 Spark 函数提供者
//     *
//     * @param methodName 方法名称
//     * @return 匹配的 Spark 函数提供者
//     */
//    public Optional<JQuickFunctionProvider> getSparkProviderByMethod(String methodName) {
//        return getProviderByMethod(COMPUTE_TYPE_SPARK, methodName);
//    }
//
//    /**
//     * 获取所有可用的 Spark 方法名称列表
//     */
//    public List<String> getAvailableSparkMethodNames() {
//        return getAvailableMethodNames(COMPUTE_TYPE_SPARK);
//    }
//
//
//    /**
//     * 根据方法名执行 Flink 计算
//     *
//     * @param methodName 方法名称
//     * @param args       参数列表
//     * @return 执行结果
//     * @throws NoSuchElementException 如果没有找到对应方法名的提供者
//     */
//    public Object executeFlinkByMethod(String methodName, List<?> args) {
//        JQuickFunctionProvider provider = getFlinkProviderByMethod(methodName)
//                .orElseThrow(() -> new NoSuchElementException("No Flink function provider found for method: " + methodName));
//        return provider.apply(args);
//    }
//
//    /**
//     * 根据方法名执行 Flink 计算 - 单参数版本
//     */
//    public Object executeFlinkByMethodOne(String methodName, Object arg) {
//        JQuickFunctionProvider provider = getFlinkProviderByMethod(methodName)
//                .orElseThrow(() -> new NoSuchElementException("No Flink function provider found for method: " + methodName));
//        return provider.applyOne(arg);
//    }
//
//    /**
//     * 根据方法名执行 Flink 计算 - 双参数版本
//     */
//    public Object executeFlinkByMethodTwo(String methodName, Object arg1, Object arg2) {
//        JQuickFunctionProvider provider = getFlinkProviderByMethod(methodName)
//                .orElseThrow(() -> new NoSuchElementException("No Flink function provider found for method: " + methodName));
//        return provider.applyTwo(arg1, arg2);
//    }
//
//    /**
//     * 根据方法名执行 Flink 计算 - 三参数版本
//     */
//    public Object executeFlinkByMethodThree(String methodName, Object arg1, Object arg2, Object arg3) {
//        JQuickFunctionProvider provider = getFlinkProviderByMethod(methodName)
//                .orElseThrow(() -> new NoSuchElementException("No Flink function provider found for method: " + methodName));
//        return provider.applyThree(arg1, arg2, arg3);
//    }
//
//    /**
//     * 根据方法名获取 Flink 函数提供者
//     *
//     * @param methodName 方法名称
//     * @return 匹配的 Flink 函数提供者
//     */
//    public Optional<JQuickFunctionProvider> getFlinkProviderByMethod(String methodName) {
//        return getProviderByMethod(COMPUTE_TYPE_FLINK, methodName);
//    }
//
//    /**
//     * 获取所有可用的 Flink 方法名称列表
//     */
//    public List<String> getAvailableFlinkMethodNames() {
//        return getAvailableMethodNames(COMPUTE_TYPE_FLINK);
//    }
//
//    /**
//     * 根据计算类型和方法名获取函数提供者
//     *
//     * @param computeType 计算类型
//     * @param methodName  方法名称
//     * @return 匹配的函数提供者
//     */
//    private Optional<JQuickFunctionProvider> getProviderByMethod(String computeType, String methodName) {
//        Map<String, JQuickFunctionProvider<?, ?>> typeMethodCache = methodCache.get(computeType);
//        if (typeMethodCache == null) {
//            return Optional.empty();
//        }
//        JQuickFunctionProvider<?, ?> cached = typeMethodCache.get(methodName);
//        if (cached != null) {
//            return Optional.of(cached);
//        }
//        return loadProvidersByComputeType(computeType).stream()
//                .filter(provider -> {
//                    JQuickComputeTypeImpl computeTypeImpl = provider.getType();
//                    if (computeTypeImpl == null) {
//                        return false;
//                    }
//                    String method = computeTypeImpl.getMethod();
//                    return method != null && method.equalsIgnoreCase(methodName);
//                })
//                .findFirst()
//                .map(provider -> {
//                    typeMethodCache.put(methodName, provider);
//                    return provider;
//                });
//    }
//
//    /**
//     * 获取指定计算类型的所有可用方法名称列表
//     */
//    private List<String> getAvailableMethodNames(String computeType) {
//        return loadProvidersByComputeType(computeType).stream()
//                .map(provider -> {
//                    JQuickComputeTypeImpl computeTypeImpl = provider.getType();
//                    return computeTypeImpl != null ? computeTypeImpl.getMethod() : null;
//                })
//                .filter(Objects::nonNull)
//                .collect(Collectors.toList());
//    }
//
//    /**
//     * 判断提供者是否匹配指定的计算类型
//     */
//    private boolean matchesComputeType(JQuickFunctionProvider<?, ?> provider, String computeType) {
//        JQuickComputeTypeImpl computeTypeImpl = provider.getType();
//        if (computeTypeImpl == null) {
//            return false;
//        }
//        JQuickProviderType providerType = computeTypeImpl.getType();
//        if (providerType == null) {
//            return false;
//        }
//        return providerType.getType().equalsIgnoreCase(computeType);
//    }
//
//    /**
//     * 获取提供者的优先级
//     */
//    private int getProviderPriority(JQuickFunctionProvider<?, ?> provider) {
//        Priority priority = provider.getClass().getAnnotation(Priority.class);
//        if (priority != null) {
//            return priority.value();
//        }
//        return PriorityConstants.DEFAULT;
//    }
//
//    /**
//     * 初始化所有已加载的提供者
//     */
//    public void initAllProviders() {
//        loadAllProviders().forEach(JQuickFunctionProvider::init);
//    }
//
//    /**
//     * 初始化指定类型的提供者
//     */
//    public void initProvidersByType(String computeType) {
//        loadProvidersByComputeType(computeType).forEach(JQuickFunctionProvider::init);
//    }
//
//    /**
//     * 销毁所有已加载的提供者
//     */
//    public void destroyAllProviders() {
//        loadAllProviders().forEach(JQuickFunctionProvider::destroy);
//    }
//
//    /**
//     * 销毁指定类型的提供者
//     */
//    public void destroyProvidersByType(String computeType) {
//        loadProvidersByComputeType(computeType).forEach(JQuickFunctionProvider::destroy);
//    }
//
//    /**
//     * 刷新缓存 - 重新加载所有服务
//     */
//    public void refresh() {
//        functionProviderCache.clear();
//        for (Map<String, JQuickFunctionProvider<?, ?>> map : methodCache.values()) {
//            map.clear();
//        }
//        ServiceLoader.reload(JQuickFunctionProvider.class);
//    }
//
//    /**
//     * 刷新指定类型的缓存
//     */
//    public void refresh(String computeType) {
//        functionProviderCache.remove(computeType);
//        Map<String, JQuickFunctionProvider<?, ?>> typeMethodCache = methodCache.get(computeType);
//        if (typeMethodCache != null) {
//            typeMethodCache.clear();
//        }
//    }
//
//    /**
//     * 打印所有提供者的信息（调试用）
//     */
//    public void printAllProvidersInfo() {
//        System.out.println("========== JQuickFunctionProvider 加载信息 ==========");
//        for (String computeType : Arrays.asList(COMPUTE_TYPE_JAVA, COMPUTE_TYPE_SPARK, COMPUTE_TYPE_FLINK)) {
//            List<JQuickFunctionProvider> providers = loadProvidersByComputeType(computeType);
//            System.out.println("\n[" + computeType + "] 类型提供者数量: " + providers.size());
//            for (int i = 0; i < providers.size(); i++) {
//                JQuickFunctionProvider provider = providers.get(i);
//                int priority = getProviderPriority(provider);
//                String method = "";
//                JQuickComputeTypeImpl computeTypeImpl = provider.getType();
//                if (computeTypeImpl != null) {
//                    method = computeTypeImpl.getMethod();
//                }
//               // System.out.printf("  %d. %s (方法: %s, 优先级: %d)%n", i + 1, provider.getClass().getSimpleName(), method, priority);
//            }
//        }
//        System.out.println("====================================================");
//    }
//
//    /**
//     * 获取所有已注册的计算类型
//     */
//    public Set<String> getRegisteredComputeTypes() {
//        Set<String> types = new HashSet<>();
//        for (JQuickFunctionProvider<?, ?> provider : loadAllProviders()) {
//            JQuickComputeTypeImpl computeType = provider.getType();
//            if (computeType != null && computeType.getType() != null) {
//                types.add(computeType.getType().getType());
//            }
//        }
//        return types;
//    }
//
//    /**
//     * 检查指定计算类型是否有可用的提供者
//     */
//    public boolean hasProviderForType(String computeType) {
//        return !loadProvidersByComputeType(computeType).isEmpty();
//    }
//
////    /**
////     * 获取提供者数量统计
////     */
////    public Map<String, Integer> getProviderStatistics() {
////        Map<String, Integer> stats = new LinkedHashMap<>();
////        stats.put(COMPUTE_TYPE_JAVA, getJavaProviders().size());
////        stats.put(COMPUTE_TYPE_SPARK, getSparkProviders().size());
////        stats.put(COMPUTE_TYPE_FLINK, getFlinkProviders().size());
////        stats.put("TOTAL", loadAllProviders().size());
////        return stats;
////    }
//
//    /**
//     * 获取方法统计
//     */
//    public Map<String, List<String>> getMethodStatistics() {
//        Map<String, List<String>> stats = new LinkedHashMap<>();
//        stats.put(COMPUTE_TYPE_JAVA, getAvailableJavaMethodNames());
//        stats.put(COMPUTE_TYPE_SPARK, getAvailableSparkMethodNames());
//        stats.put(COMPUTE_TYPE_FLINK, getAvailableFlinkMethodNames());
//        return stats;
//    }
//}