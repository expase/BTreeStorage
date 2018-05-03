package com.github.storage.table;

import java.nio.ByteBuffer;
import java.util.List;

public class Record {
    private List<Field> values;

    public Record(List<Field> values) {
        this.values = values;
    }

    public byte[] toBytes(Schema schema) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(schema.getSizeInBytes());
        for (Field value : values) {
            byteBuffer.put(value.toBytes());
        }
        return byteBuffer.array();
    }
}
