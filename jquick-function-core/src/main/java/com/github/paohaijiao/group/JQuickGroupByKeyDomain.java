package com.github.paohaijiao.group;

import com.github.paohaijiao.statement.JQuickRow;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class JQuickGroupByKeyDomain {

    private final List<Object> values;

    public  JQuickGroupByKeyDomain(JQuickRow row, List<String> columns) {
        this.values = columns.stream().map(row::get).collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JQuickGroupByKeyDomain groupKey = (JQuickGroupByKeyDomain) o;
        return Objects.equals(values, groupKey.values);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(values);
    }
}
