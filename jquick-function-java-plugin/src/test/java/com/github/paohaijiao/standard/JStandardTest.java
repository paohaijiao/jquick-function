package com.github.paohaijiao.standard;

import com.github.paohaijiao.provider.JQuickFunctionProvider;
import com.github.paohaijiao.provider.standard.impl.*;
import com.github.paohaijiao.statement.JQuickColumnMeta;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;
import com.github.paohaijiao.transform.JQuickSelectTransformer;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class JStandardTest {

    private JQuickDataSet originalDataSet;

    @Before
    public void setUp() {
        List<JQuickColumnMeta> columns = Arrays.asList(
                new JQuickColumnMeta("id", Integer.class, "source"),
                new JQuickColumnMeta("name", String.class, "source"),
                new JQuickColumnMeta("age", Integer.class, "source"),
                new JQuickColumnMeta("salary", Double.class, "source"),
                new JQuickColumnMeta("department", String.class, "source"),
                new JQuickColumnMeta("bonus", Double.class, "source"),
                new JQuickColumnMeta("active", Boolean.class, "source")
        );

        List<JQuickRow> rows = Arrays.asList(
                createRow(1, "张三", 25, 8000.0, "技术部", 2000.0, true),
                createRow(2, "李四", 30, 10000.0, "市场部", 3000.0, true),
                createRow(3, "王五", 28, 9000.0, "技术部", 2500.0, false),
                createRow(4, "赵六", 35, 12000.0, "销售部", 4000.0, true),
                createRow(5, "钱七", 22, 6000.0, "市场部", 1500.0, true)
        );

        originalDataSet = new JQuickDataSet(columns, rows);
    }

    private JQuickRow createRow(Object... values) {
        JQuickRow row = new JQuickRow();
        row.put("id", values[0]);
        row.put("name", values[1]);
        row.put("age", values[2]);
        row.put("salary", values[3]);
        row.put("department", values[4]);
        row.put("bonus", values[5]);
        row.put("active", values[6]);
        return row;
    }


    @Test
    public void test() {
        List<JQuickFunctionProvider<?, ?>> providers = Arrays.asList(
                JQuickColumnProvider.asString("name", "员工姓名"),
                JQuickColumnProvider.asInt("age", "年龄"),
                JQuickColumnProvider.asString("department", "部门")
        );

        JQuickSelectTransformer transformer = new JQuickSelectTransformer(originalDataSet, providers);
        JQuickDataSet result = transformer.transform();
        result.printTable();

    }

    @Test
    public void testKeepOriginalColumns() {
        List<JQuickFunctionProvider<?, ?>> providers = Arrays.asList(
                // 计算年薪 = 月薪 * 12
                new CompositeProvider<>("annualSalary", Double.class,
                        row -> {
                            Double salary = row.getAs("salary", Double.class);
                            return salary != null ? salary * 12 : 0.0;
                        }, "salary"),
                // 计算总收入 = 月薪 + 奖金
                new CompositeProvider<>("totalIncome", Double.class,
                        row -> {
                            Double salary = row.getAs("salary", Double.class);
                            Double bonus = row.getAs("bonus", Double.class);
                            return (salary != null ? salary : 0.0) + (bonus != null ? bonus : 0.0);
                        }, "salary", "bonus"),
                // 添加状态描述
                new JQuickConditionalProvider<String>("status", String.class, "未知")
                        .whenEquals("active", true, "在职")
                        .whenEquals("active", false, "离职")
        );

        JQuickSelectTransformer transformer = JQuickSelectTransformer
                .select(originalDataSet, providers.toArray(new JQuickFunctionProvider[0]))
                .keepOriginalColumns(true);  // 保留原始所有列

        JQuickDataSet result = transformer.transform();
        result.printTable();
    }

    /**
     * 测试3: 重命名列 + 排除列
     */
    @Test
    public void testAliasAndExclude() {
        System.out.println("========== 测试3: 重命名列 + 排除列 ==========");
        List<JQuickFunctionProvider<?, ?>> providers = Arrays.asList(
                JQuickColumnProvider.asString("name", "employee_name"),
                JQuickColumnProvider.asDouble("salary", "base_salary")
        );

        JQuickSelectTransformer transformer = JQuickSelectTransformer
                .select(originalDataSet, providers.toArray(new JQuickFunctionProvider[0]))
                .keepOriginalColumns(true)
                .alias("id", "employee_id")      // 重命名列
                .alias("department", "dept")      // 重命名列
                .exclude("bonus", "active");      // 排除 bonus 和 active 列

        JQuickDataSet result = transformer.transform();
        result.printTable();
    }

    /**
     * 测试4: 类型转换和默认值处理
     */
    @Test
    public void testTypeConversionAndDefaultValue() {
        List<JQuickFunctionProvider<?, ?>> providers = Arrays.asList(
                new JQuickDefaultValueProvider<>("salary", "salary_level", String.class, "0")
                        .converter(value -> {
                            double salary = ((Number) value).doubleValue();
                            if (salary >= 10000) return "高";
                            if (salary >= 8000) return "中";
                            return "低";
                        }),
                JQuickConstantProvider.string("company", "XX科技有限公司"),
                // 组合列 - 全名
                new CompositeProvider<>("fullName", String.class,
                        row -> row.getString("name") + "(" + row.getInt("age") + "岁)", "name", "age")
        );
        JQuickSelectTransformer transformer = JQuickSelectTransformer
                .select(originalDataSet, providers.toArray(new JQuickFunctionProvider[0]))
                .keepOriginalColumns(false);  // 只保留 provider 定义的列

        JQuickDataSet result = transformer.transform();
        result.printTable();
    }

    /**
     * 测试5: 计算字段 - 多种表达式
     */
    @Test
    public void testComputedFields() {
        List<JQuickFunctionProvider<?, ?>> providers = Arrays.asList(
                // 字符串拼接
                CompositeProvider.concat("dept_info", "-", "department", "name"),
                // 数值求和
                CompositeProvider.sum("total_compensation", "salary", "bonus"),
                // 自定义计算 - 税后工资
                new CompositeProvider<>("afterTax", Double.class,
                        row -> {
                            Double salary = row.getAs("salary", Double.class);
                            return salary != null ? salary * 0.85 : 0.0;
                        }, "salary")
        );

        JQuickSelectTransformer transformer = JQuickSelectTransformer
                .select(originalDataSet, providers.toArray(new JQuickFunctionProvider[0]))
                .keepOriginalColumns(true);

        JQuickDataSet result = transformer.transform();
        result.printTable();
    }

    /**
     * 测试6: 条件字段 - 复杂业务逻辑
     */
    @Test
    public void testConditionalFields() {
        List<JQuickFunctionProvider<?, ?>> providers = Arrays.asList(
                // 年龄分组
                new JQuickConditionalProvider<String>("ageGroup", String.class, "未知")
                        .when(row -> row.getInt("age") < 25, "青年")
                        .when(row -> row.getInt("age") >= 25 && row.getInt("age") < 30, "壮年")
                        .when(row -> row.getInt("age") >= 30, "中年"),
                // 薪资等级 + 活动状态组合
                new JQuickConditionalProvider<String>("recommendation", String.class, "无")
                        .when(row -> row.getDouble("salary") > 9000 && Boolean.TRUE.equals(row.getBoolean("active")), "推荐晋升")
                        .when(row -> row.getDouble("salary") > 7000, "表现良好")
                        .when(row -> Boolean.FALSE.equals(row.getBoolean("active")), "需关注")
        );

        JQuickSelectTransformer transformer = JQuickSelectTransformer
                .select(originalDataSet, providers.toArray(new JQuickFunctionProvider[0]))
                .keepOriginalColumns(true)
                .exclude("bonus");

        JQuickDataSet result = transformer.transform();
        result.printTable();
    }

    /**
     * 测试7: 链式转换 - 多次转换
     */
    @Test
    public void testChainedTransformations() {
        System.out.println("========== 测试7: 链式转换 ==========");

        // 第一次转换：选择基础列
        List<JQuickFunctionProvider<?, ?>> firstProviders = Arrays.asList(
                JQuickColumnProvider.asString("name", "name"),
                JQuickColumnProvider.asInt("age", "age"),
                JQuickColumnProvider.asDouble("salary", "salary")
        );

        JQuickDataSet firstResult = new JQuickSelectTransformer(originalDataSet, firstProviders)
                .transform();

        System.out.println("--- 第一次转换后 ---");
        firstResult.printTable();

        // 第二次转换：基于第一次结果继续处理
        List<JQuickFunctionProvider<?, ?>> secondProviders = Arrays.asList(
                new CompositeProvider<>("salaryLevel", String.class,
                        row -> {
                            Double salary = row.getAs("salary", Double.class);
                            if (salary >= 10000) return "A级";
                            if (salary >= 8000) return "B级";
                            return "C级";
                        }, "salary"),
                JQuickColumnProvider.asString("name", "employeeName")
        );

        JQuickDataSet finalResult = JQuickSelectTransformer
                .select(firstResult, secondProviders.toArray(new JQuickFunctionProvider[0]))
                .keepOriginalColumns(true)
                .transform();

        System.out.println("--- 第二次转换后 ---");
        finalResult.printTable();
    }

    /**
     * 测试8: 自定义 Provider
     */
    @Test
    public void testCustomProvider() {
        System.out.println("========== 测试8: 自定义 Provider ==========");

        // 自定义匿名 Provider
        JQuickFunctionProvider<JQuickRow, String> customProvider = new JQuickFunctionProvider<JQuickRow, String>() {
            @Override
            public String apply(JQuickRow row) {
                String name = row.getString("name");
                Double salary = row.getDouble("salary");
                return String.format("%s (¥%.0f)", name, salary);
            }

            @Override
            public String getTargetField() {
                return "displayInfo";
            }

            @Override
            public Class<?> getTargetClass() {
                return String.class;
            }
        };

        List<JQuickFunctionProvider<?, ?>> providers = Arrays.asList(
                JQuickColumnProvider.asInt("id", "id"),
                customProvider
        );

        JQuickDataSet result = new JQuickSelectTransformer(originalDataSet, providers)
                .transform();

        result.printTable();
    }

    /**
     * 测试9: 使用 andThen 进行后处理转换
     */
    @Test
    public void testAndThenTransformation() {
        JQuickColumnProvider<String> nameProvider = JQuickColumnProvider.asString("name", "name");
        JQuickColumnProvider<Integer> ageProvider = JQuickColumnProvider.asInt("age", "age");
        List<JQuickFunctionProvider<?, ?>> providers = Arrays.asList(
                nameProvider,
                ageProvider
        );

        JQuickDataSet result = new JQuickSelectTransformer(originalDataSet, providers)
                .keepOriginalColumns(false)
                .transform();

        result.printTable();
        System.out.println("转换完成，共 " + result.size() + " 行，" + result.getColumns().size() + " 列");
    }

    /**
     * 测试10: 空值处理
     */
    @Test
    public void testNullHandling() {
        System.out.println("========== 测试10: 空值处理 ==========");

        // 创建包含空值的数据集
        JQuickRow nullRow = new JQuickRow();
        nullRow.put("id", 99);
        nullRow.put("name", null);
        nullRow.put("age", null);
        nullRow.put("salary", null);
        nullRow.put("department", "测试部");

        List<JQuickRow> rowsWithNull = new java.util.ArrayList<>(originalDataSet.getRows());
        rowsWithNull.add(nullRow);

        List<JQuickColumnMeta> columns = originalDataSet.getColumns();
        JQuickDataSet dataSetWithNull = new JQuickDataSet(columns, rowsWithNull);

        List<JQuickFunctionProvider<?, ?>> providers = Arrays.asList(
                new JQuickDefaultValueProvider<>("name", "displayName", String.class, "匿名用户"),
                new JQuickDefaultValueProvider<>("age", "displayAge", Integer.class, 0),
                new JQuickDefaultValueProvider<>("salary", "displaySalary", Double.class, 0.0)
                        .converter(value -> ((Number) value).doubleValue()),
                JQuickConstantProvider.string("defaultDept", "默认部门")
        );

        JQuickDataSet result = new JQuickSelectTransformer(dataSetWithNull, providers)
                .keepOriginalColumns(false)
                .transform();

        result.printTable();
    }

}
