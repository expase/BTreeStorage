package com.github.storage.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;


public class Page {
    public static final int PAGE_SIZE = 4096;


    private MappedByteBuffer data;
    private int pageNum;

    public Page(FileChannel channel,int blockNum,int pageNum) {
        this.pageNum = pageNum;
        try {
            this.data = channel.map(FileChannel.MapMode.READ_WRITE, blockNum * PAGE_SIZE, PAGE_SIZE);
        } catch(IOException e) {
            throw new PageException("Can't mmap page: " + pageNum + "at block: " + blockNum + " ; " , e);
        }
    }


    public byte[] readBytes(int position, int num) {
        assert(position + num < PAGE_SIZE);
        byte[] data = new byte[num];
        readBytes(position, num, data);
        return data;
    }

    public void readBytes(int position, int num, byte[] buf) {
        assert(position + num < PAGE_SIZE);
        assert(buf.length > num);

        data.position(position);
        data.get(buf, 0, num);
    }

    public byte[] readBytes() {
        return readBytes(0, PAGE_SIZE);
    }

    public byte readByte(int position) {
        assert(position > 0 && position < PAGE_SIZE);
        return data.get(position);
    }

    public int readInt(int position) {
        assert(position > 0 && position < PAGE_SIZE);
        return data.getInt(position);
    }

    public short readShort(int pos) {
        assert(pos > 0 && pos < PAGE_SIZE);
        return data.getShort(pos);
    }

    public void clean() {
        byte[] zeros = new byte[PAGE_SIZE];
        writeBytes(0, PAGE_SIZE, zeros);
    }


    public void writeBytes(int position, int num, byte[] buf) {
        assert(num > 0 && num < buf.length);
        assert(position > 0);
        assert(num + position < PAGE_SIZE);
        data.position(position);
        data.put(buf, 0, num);
    }

    public void writeByte(int position,byte buf) {
        assert (position < PAGE_SIZE);
        data.put(position, buf);
    }
    public void flush() {
        data.force();
    }

    public ByteBuffer getByteBuffer() {
        data.position(0);
        return data;
    }

    public int getPageNum() {
        return pageNum;
    }

    public void wipe() {
        byte[] zeros = new byte[Page.PAGE_SIZE];
        this.writeBytes(0, Page.PAGE_SIZE, zeros);
    }
}
