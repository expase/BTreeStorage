package com.github.storage.table;

import com.github.storage.DatabaseException;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Schema {
    private List<String> fieldNames;
    private List<Type> fieldTypes;
    private int sizeInBytes;

    public Schema(List<String> fieldNames, List<Type> fieldTypes) {
        assert(fieldNames.size() == fieldTypes.size());
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;

        sizeInBytes = 0;
        for (Type t : fieldTypes) {
            sizeInBytes += t.getSizeInBytes();
        }
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public List<Type> getFieldTypes() {
        return fieldTypes;
    }

    public int getSizeInBytes() {
        return sizeInBytes;
    }

    public byte[] toBytes() {

        // First, we compute the number of bytes we need to serialize the schema.
        int size = Integer.BYTES; // The length of the schema.
        for (int i = 0; i < fieldNames.size(); ++i) {
            size += Integer.BYTES; // The length of the field name.
            size += fieldNames.get(i).length(); // The field name.
            size += fieldTypes.get(i).toBytes().length; // The type.
        }

        // Then we serialize it.
        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.putInt(fieldNames.size());
        for (int i = 0; i < fieldNames.size(); ++i) {
            buf.putInt(fieldNames.get(i).length());
            buf.put(fieldNames.get(i).getBytes(Charset.forName("UTF-8")));
            buf.put(fieldTypes.get(i).toBytes());
        }
        return buf.array();
    }

    public static Schema fromBytes(ByteBuffer buf) {
        int size = buf.getInt();
        List<String> fieldNames = new ArrayList<>();
        List<Type> fieldTypes = new ArrayList<>();
        for (int i = 0; i < size; ++i) {
            int fieldSize = buf.getInt();
            byte[] bytes = new byte[fieldSize];
            buf.get(bytes);
            fieldNames.add(new String(bytes, Charset.forName("UTF-8")));
            fieldTypes.add(Type.fromBytes(buf));
        }
        return new Schema(fieldNames, fieldTypes);
    }

    @Override
    public String toString() {
        String s = "(";
        for (int i = 0; i < fieldNames.size(); ++i) {
            s += String.format("%s: %s", fieldNames.get(i), fieldTypes.get(i));
            if (i != fieldNames.size()) {
                s += ", ";
            }
        }
        s += ")";
        return s;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof Schema)) {
            return false;
        }
        Schema s = (Schema) o;
        return fieldNames.equals(s.fieldNames) && fieldTypes.equals(s.fieldTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldNames, fieldTypes);
    }


    public Record verify(List<Field> values) throws DatabaseException {
        if (values.size() != fieldNames.size()) {
            String err = String.format("Expected %d values, but got %d.",
                    fieldNames.size(), values.size());
            throw new DatabaseException(err);
        }

        for (int i = 0; i < values.size(); ++i) {
            Type actual = values.get(i).type();
            Type expected = fieldTypes.get(i);
            if (!actual.equals(expected)) {
                String err = String.format(
                        "Expected field %d to be of type %s, but got value of type %s.",
                        i, expected, actual);
                throw new DatabaseException(err);
            }
        }

        return new Record(values);
    }

}
