package com.github.storage.table;

import java.nio.ByteBuffer;
import java.util.Objects;

public class Type {

    private TypeId typeId;
    private int sizeInBytes;

    private Type(TypeId typeId, int sizeInBytes) {
        this.typeId = typeId;
        this.sizeInBytes = sizeInBytes;
    }


    public static Type intType() {
        return new Type(TypeId.INT, Integer.BYTES);
    }


    public static Type stringType(int n) {
        assert n > 0;
        return new Type(TypeId.STRING, n);
    }

    public TypeId getTypeId() {
        return typeId;
    }

    public int getSizeInBytes() {
        return sizeInBytes;
    }

    public byte[] toBytes() {

        ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES * 2);
        buf.putInt(typeId.ordinal());
        buf.putInt(sizeInBytes);
        return buf.array();
    }

    public static Type fromBytes(ByteBuffer buf) {
        int ordinal = buf.getInt();
        int sizeInBytes = buf.getInt();
        if (ordinal == TypeId.INT.ordinal()) {
            assert(sizeInBytes == Integer.BYTES);
            return Type.intType();
        } else if (ordinal == TypeId.STRING.ordinal()) {
            return Type.stringType(sizeInBytes);
        } else {
            String err = String.format("Unknown TypeId ordinal %d.", ordinal);
            throw new IllegalArgumentException(err);
        }
    }

    @Override
    public String toString() {
        return String.format("(%s, %d)", typeId.toString(), sizeInBytes);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof Type)) {
            return false;
        }
        Type t = (Type) o;
        return typeId.equals(t.typeId) && sizeInBytes == t.sizeInBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeId, sizeInBytes);
    }
}
