package com.github.storage.table;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public abstract class Field implements Comparable<Field> {
    public abstract Type type();

    public abstract int getInt();

    public abstract String getString();

    public abstract byte[] toBytes();

    public static Field fromBytes(ByteBuffer buf, Type type) {
        switch (type.getTypeId()) {

            case INT: {
                return new IntField(buf.getInt());
            }
            case STRING: {
                byte[] bytes = new byte[type.getSizeInBytes()];
                buf.get(bytes);
                String s = new String(bytes, Charset.forName("UTF-8"));
                return new StringField(s, type.getSizeInBytes());
            }
            default: {
                String err = String.format("Unhandled TypeId %s.",
                        type.getTypeId().toString());
                throw new IllegalArgumentException(err);
            }
        }
    }
}
