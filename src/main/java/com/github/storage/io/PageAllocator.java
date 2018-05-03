package com.github.storage.io;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class PageAllocator {
    private static final int HEAD_PAGE_NUM = Page.PAGE_SIZE / (Integer.SIZE / Byte.SIZE);
    private static final int CACHE_SIZE = 1024;


    private static LRUCache<Long, Page> pageLRU = new LRUCache<Long, Page>(CACHE_SIZE);

    private Page masterPage;
    private FileChannel channel;
    private int numPages;
    private int allocID;
    private static AtomicInteger pACounter = new AtomicInteger(0);

    public PageAllocator(String fileName,boolean wipe) {
        try {
            channel = new RandomAccessFile(fileName, "rw").getChannel();
        } catch(FileNotFoundException e) {
            throw new RuntimeException("could not open file " + fileName, e);
        }
        this.allocID = pACounter.getAndIncrement();

        this.masterPage = new Page(channel, 0, -1);
        if (wipe) {
            // Nukes masterPage and headerPages
            byte[] masterBytes = this.masterPage.readBytes();
            IntBuffer ib = ByteBuffer.wrap(masterBytes).asIntBuffer();

            int[] pageCounts = new int[ib.capacity()];
            ib.get(pageCounts);

            this.numPages = 0;

            for (int i = 0; i < HEAD_PAGE_NUM; i++) {
                if (pageCounts[i] > 0) {
                    getHeadPage(i).wipe();
                }
            }

            this.masterPage.wipe();
        }

        int[] pageCounts = getPageCounts();
        this.numPages = 0;
        for(int i = 0;i < HEAD_PAGE_NUM; i++) {
            this.numPages += pageCounts[i];
        }
    }

    private int[] getPageCounts() {
        byte[] masterBytes = masterPage.readBytes();
        IntBuffer ib = ByteBuffer.wrap(masterBytes).asIntBuffer();
        int[] pageCounts = new int[ib.capacity()];
        ib.get(pageCounts);
        return pageCounts;
    }
    public int allocPage() {
        int[] pageCounts = getPageCounts();

        Page headerPage = null;
        int headerIndex = -1;
        for(int i = 0;i < HEAD_PAGE_NUM; i++) {
            if(pageCounts[i] < Page.PAGE_SIZE) {
                headerPage = getHeadPage(i);
                headerIndex = i;
                break;
            }
        }

        if(headerPage == null) {
            throw new PageException("No more free page for allocate");
        }

        byte[] headerBytes = headerPage.readBytes();
        int pageIndex = -1;

        for (int i = 0; i < Page.PAGE_SIZE; i++) {
            if (headerBytes[i] == 0) {
                pageIndex = i;
                break;
            }
        }

        int newCount = pageCounts[headerIndex] + 1;
        byte[] newCountBytes = ByteBuffer.allocate(4).putInt(newCount).array();
        this.masterPage.writeBytes(headerIndex*4, 4, newCountBytes);
        headerPage.writeByte(pageIndex, (byte) 1);

        this.masterPage.flush();
        headerPage.flush();


        int pageNum = headerIndex * Page.PAGE_SIZE + pageIndex;
        this.numPages += 1;

        return pageNum;

    }


    public  Page fetchPage(int pageNum) {
        assert (pageNum >= 0);



        if (pageLRU.containsKey(translatePageNum(pageNum))) {
            return pageLRU.get(translatePageNum(pageNum));
        }

        int headPageIndex = pageNum/Page.PAGE_SIZE;

        assert(headPageIndex < HEAD_PAGE_NUM);


        byte[] headCountBytes = this.masterPage.readBytes(headPageIndex*4, 4);
        int headCount = ByteBuffer.wrap(headCountBytes).getInt();

        if (headCount < 1) {
            throw new PageException("invalid page number -- page not allocated");
        }

        Page headPage = getHeadPage(headPageIndex);

        int dataPageIndex = pageNum % Page.PAGE_SIZE;

        byte validByte = headPage.readByte(dataPageIndex);

        if (validByte == 0) {
            throw new PageException("invalid page number -- page not allocated");
        }

        int dataBlockID = 2 + headPageIndex*(Page.PAGE_SIZE + 1) + dataPageIndex;
        Page dataPage = new Page(this.channel, dataBlockID, pageNum);

        pageLRU.put(translatePageNum(pageNum), dataPage);

        return dataPage;
    }

    private long translatePageNum(int pageNum) {
        return (((long) this.allocID) << 32) | (((long) pageNum) & 0xFFFFFFFFL);
    }


    private  Page getHeadPage(int headIndex) {
        int headBlockID = 1 + headIndex*(Page.PAGE_SIZE + 1);
        return new Page(this.channel, headBlockID, -1);
    }
    private static class LRUCache<k extends Long, v extends Page> extends LinkedHashMap<k, v> {
        private int cacheSize;

        public LRUCache(int cacheSize) {
            super(16, 0.75f, true);
            this.cacheSize = cacheSize;
        }

        protected boolean removeEldestEntry(Map.Entry<k, v> eldest) {
            if (size() > cacheSize) {
                eldest.getValue().flush();
                return true;
            }
            return false;
        }
    }

    public int getNumPages() {
        return numPages;
    }

    public static void main(String[] args) {
        PageAllocator allocator = new PageAllocator("/Users/zzxia/tmp/page.table", true);
        Page page = allocator.fetchPage(allocator.allocPage());

        System.out.println(allocator.getNumPages());
    }

}
