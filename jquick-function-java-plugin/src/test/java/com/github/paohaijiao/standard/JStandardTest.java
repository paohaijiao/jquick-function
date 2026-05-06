package com.github.paohaijiao.standard;

import com.github.paohaijiao.provider.standard.JQuickBaseStandardProvider;
import com.github.paohaijiao.provider.standard.impl.JQuickStandardConcatProvider;
import com.github.paohaijiao.provider.standard.impl.JQuickStandardSumFieldsProvider;
import com.github.paohaijiao.provider.standard.impl.JQuickStandardToIntegerProvider;
import com.github.paohaijiao.statement.JQuickColumnMeta;
import com.github.paohaijiao.statement.JQuickDataSet;
import com.github.paohaijiao.statement.JQuickRow;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class JStandardTest {

    @Test
    public void test() {
        JQuickDataSet dataSet = createDataSet();
        JQuickStandardToIntegerProvider ageToInt = new JQuickStandardToIntegerProvider("age", "age_int");
        JQuickStandardConcatProvider nameConcat = new JQuickStandardConcatProvider(
                Arrays.asList("name", "age_int"), "name_age", " - "
        );
        JQuickStandardSumFieldsProvider sumProvider = new JQuickStandardSumFieldsProvider(
                Arrays.asList("salary", "bonus"), "total"
        );
        JQuickDataSet result1 = ageToInt.transform(dataSet);
        JQuickDataSet result2 = JQuickBaseStandardProvider.transformChain(
                dataSet, ageToInt, nameConcat, sumProvider
        );

        JQuickDataSet result3 = JQuickBaseStandardProvider.transformChain(
                dataSet, false, ageToInt, nameConcat, sumProvider
        );

        System.out.println("=== 原始数据 ===");
        dataSet.printTable();

        System.out.println("\n=== 转换结果（保留原始列）===");
        result2.printTable();

        System.out.println("\n=== 转换结果（删除原始列）===");
        result3.printTable();
    }
    private static JQuickDataSet createDataSet() {
        List<JQuickColumnMeta> columns = Arrays.asList(
                new JQuickColumnMeta("name", String.class, "source"),
                new JQuickColumnMeta("age", String.class, "source"),
                new JQuickColumnMeta("salary", String.class, "source"),
                new JQuickColumnMeta("bonus", String.class, "source")
        );
        List<JQuickRow> rows = Arrays.asList(
                createRow("Alice", "25", "100", "50"),
                createRow("Bob", "30", "200", "75"),
                createRow("Charlie", "35", "300", "100"),
                createRow("David", "invalid", "400", "null")
        );

        return new JQuickDataSet(columns, rows);
    }

    private static JQuickRow createRow(String name, String age, String salary, String bonus) {
        JQuickRow row = new JQuickRow();
        row.put("name", name);
        row.put("age", age);
        row.put("salary", salary);
        row.put("bonus", bonus);
        return row;
    }
}
