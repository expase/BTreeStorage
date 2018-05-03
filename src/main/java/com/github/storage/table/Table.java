package com.github.storage.table;


import com.github.storage.io.Page;
import com.github.storage.io.PageAllocator;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.TreeSet;

public class Table {
    public static final String FILENAME_EXTENSION = ".table";

    private String name;
    private Schema schema;
    private PageAllocator allocator;
    private TreeSet<Integer> freePageNums = new TreeSet<>();

    private int numRecordsPerPage = 0;
    private short numRecords = 0;

    public Table(String name, Schema schema, String fileName) {
        this.name = name;
        this.schema = schema;
        this.allocator = new PageAllocator(fileName, true);
        this.numRecordsPerPage = computeNumRecordsPerPage(Page.PAGE_SIZE, schema);
        writeSchemaToHeaderPage(allocator, schema);
    }

    public Table(String name,String fileName) {
        this.name = name;
        this.allocator = new PageAllocator(fileName, false);

        Page headerPage = allocator.fetchPage(0);
        ByteBuffer buffer = headerPage.getByteBuffer();
        this.numRecords = buffer.getShort();
        this.schema = Schema.fromBytes(buffer);
    }

    private void writeSchemaToHeaderPage(PageAllocator allocator, Schema schema) {
        Page headerPage = allocator.fetchPage(allocator.allocPage());
        assert(0 == headerPage.getPageNum());
        ByteBuffer buf = headerPage.getByteBuffer();
        buf.putShort(this.numRecords);
        buf.put(schema.toBytes());
        headerPage.flush();
    }

    public RecordId addRecord(List<Field> values) {
        Record record = schema.verify(values);
        if (freePageNums.isEmpty()) {
            freePageNums.add(allocator.allocPage());
        }
        Page page = allocator.fetchPage(freePageNums.first());
        int recordNum  = page.readShort(0);
        assert(recordNum < numRecordsPerPage);
        insertRecord(page, recordNum, record);
        numRecords++;
        if(numRecords == numRecordsPerPage) {
            freePageNums.pollFirst();
        }
        page.getByteBuffer().putShort(numRecords);

        page.flush();

        return new RecordId(page.getPageNum(), (short)recordNum);

    }

    private void insertRecord(Page page, int entryNum, Record record) {
        int offset = Short.BYTES + (entryNum * schema.getSizeInBytes());
        byte[] bytes = record.toBytes(schema);
        ByteBuffer buf = page.getByteBuffer();
        buf.position(offset);
        buf.put(bytes);
    }


    private static int computeNumRecordsPerPage(int pageSize, Schema schema) {
        return (pageSize - Short.BYTES) / schema.getSizeInBytes();
    }
}
