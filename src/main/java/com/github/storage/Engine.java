package com.github.storage;

import com.github.storage.command.CreateStmt;
import com.github.storage.table.Schema;
import com.github.storage.table.Table;
import com.github.storage.table.Type;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Engine {
    private Map<String, Table> tables = new HashMap<>();
    private String fileDir;
    public Engine(String fileDir) {
        this.fileDir = fileDir;
        init();
    }
    private void init() {
        File dir = new File(fileDir);
        if(!dir.exists()) {
            dir.mkdirs();
        }

        File[] files = dir.listFiles();

        for (File f : files) {
            String fName = f.getName();
            if (fName.endsWith(Table.FILENAME_EXTENSION)) {
                int lastIndex = fName.lastIndexOf(Table.FILENAME_EXTENSION);
                String tableName = fName.substring(0, lastIndex);
                tables.put(tableName, new Table(tableName, f.toPath().toString()));
            }
        }
    }
    public Table createTable(CreateStmt stmt) {

        if (this.tables.containsKey(stmt.getTable())) {
            return tables.get(stmt.getTable());
        }
        Schema schema = new Schema(stmt.getFieldNames(), stmt.getFieldTypes());
        Path path = Paths.get(fileDir, stmt.getTable() + Table.FILENAME_EXTENSION);

        Table table = new Table(stmt.getTable(), schema,  path.toString());

        tables.put(stmt.getTable(), table);

        return table;

    }

    public static String currentPath() {
        Path currentRelativePath = Paths.get("");
        return currentRelativePath.toAbsolutePath().toString();
    }
    public static void main(String[] args) {
        Engine engine= new Engine(currentPath() + "/db");
        CreateStmt stmt = new CreateStmt();
        stmt.setFieldNames(Arrays.asList("id", "name"));
        stmt.setFieldTypes(Arrays.asList(Type.intType(), Type.stringType(30)));
        stmt.setKey("id");
        stmt.setTable("users");
        stmt.setKey("id");
        engine.createTable(stmt);


    }
}
