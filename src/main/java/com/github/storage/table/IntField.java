package com.github.storage.table;

import java.nio.ByteBuffer;

public class IntField extends Field {
    private int value;

    public IntField(int value) {
        this.value = value;
    }
    @Override
    public Type type() {
        return Type.intType();
    }

    @Override
    public int getInt() {
        return value;
    }

    @Override
    public String getString() {
        return null;
    }

    @Override
    public byte[] toBytes() {
        return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof IntField)) {
            return false;
        }
        IntField i = (IntField) o;
        return this.value == i.value;
    }

    @Override
    public int hashCode() {
        return new Integer(value).hashCode();
    }

    @Override
    public int compareTo(Field d) {
        if (!(d instanceof IntField)) {
            String err = String.format("Invalid comparison between %s and %s.",
                    toString(), d.toString());
            throw new RuntimeException(err);
        }
        IntField i = (IntField) d;
        return Integer.compare(this.value, i.value);
    }
}
