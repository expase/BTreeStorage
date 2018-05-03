package com.github.storage.table;

import java.nio.charset.Charset;

public class StringField extends Field {
    private String value;
    private int size;
    public StringField(String value,int size) {
        this.value = value;
        this.size = size;
    }
    @Override
    public Type type() {
        return Type.stringType(size);
    }

    @Override
    public int getInt() {
        return 0;
    }

    @Override
    public String getString() {
        return value;
    }

    @Override
    public byte[] toBytes() {
        return value.getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StringField)) {
            return false;
        }
        StringField s = (StringField) o;
        return this.value.equals(s.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public int compareTo(Field d) {
        if (!(d instanceof StringField)) {
            String err = String.format("Invalid comparison between %s and %s.",
                    toString(), d.toString());
            throw new RuntimeException(err);
        }
        StringField s = (StringField) d;
        return this.value.compareTo(s.value);
    }
}
