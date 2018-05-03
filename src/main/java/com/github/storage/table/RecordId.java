package com.github.storage.table;

import java.nio.ByteBuffer;
import java.util.Objects;

public class RecordId implements Comparable<RecordId> {
    private int pageNum;
    private short recordIndex;

    public RecordId(int pageNum, short recordIndex) {
        this.pageNum = pageNum;
        this.recordIndex = recordIndex;
    }

    public int getPageNum() {
        return this.pageNum;
    }

    public short getRecordIndex() {
        return this.recordIndex;
    }

    public static int getSizeInBytes() {
        // See toBytes.
        return Integer.BYTES + Short.BYTES;
    }

    public byte[] toBytes() {
        // A RecordId is serialized as its 4-byte page number followed by its
        // 2-byte short.
        return ByteBuffer.allocate(getSizeInBytes())
                .putInt(pageNum)
                .putShort(recordIndex)
                .array();
    }

    public static RecordId fromBytes(ByteBuffer buf) {
        return new RecordId(buf.getInt(), buf.getShort());
    }

    @Override
    public String toString() {
        return String.format("RecordId(%d, %d)", pageNum, recordIndex);
    }

    public String toSexp() {
        return String.format("(%d %d)", pageNum, recordIndex);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof RecordId)) {
            return false;
        }
        RecordId r = (RecordId) o;
        return pageNum == r.pageNum && recordIndex == r.recordIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pageNum, recordIndex);
    }

    @Override
    public int compareTo(RecordId r) {
        int x = Integer.compare(pageNum, r.pageNum);
        return x == 0 ? Integer.compare(recordIndex, r.recordIndex) : x;
    }

}
