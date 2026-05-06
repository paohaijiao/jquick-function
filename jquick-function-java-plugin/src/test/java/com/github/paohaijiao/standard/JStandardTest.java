package com.github.paohaijiao.standard;

import com.github.paohaijiao.provider.standard.impl.ConcatProvider;
import com.github.paohaijiao.provider.standard.impl.ToIntegerProvider;
import org.junit.Test;

import java.util.Arrays;

public class JStandardTest {

    @Test
    public void test() {
        ToIntegerProvider toInt = new ToIntegerProvider("age", "age_int");
        Integer result = toInt.apply(Arrays.asList("123"));  // 输出 123
        ConcatProvider concat = new ConcatProvider(Arrays.asList("first_name", "last_name"), "full_name", " ");
        String fullName = concat.apply(Arrays.asList("John", "Doe"));  // 输出 "John Doe"
        System.out.println(fullName);
    }
}
