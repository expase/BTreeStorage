package com.github.storage.command;

import com.github.storage.table.Type;

import java.util.List;

public class CreateStmt {
    private String table;
    private List<String> fieldNames;
    private List<Type> fieldTypes;
    private String key;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public void setFieldNames(List<String> fieldNames) {
        this.fieldNames = fieldNames;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public List<Type> getFieldTypes() {
        return fieldTypes;
    }

    public void setFieldTypes(List<Type> fieldTypes) {
        this.fieldTypes = fieldTypes;
    }
}
