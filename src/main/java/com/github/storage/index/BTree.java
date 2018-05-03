package com.github.storage.index;

import com.github.storage.io.Page;
import com.github.storage.io.PageAllocator;
import com.github.storage.table.Field;
import com.github.storage.table.RecordId;
import com.github.storage.table.Type;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class BTree {
    private Page headerPage;
    private BTreeMetadata metadata;
    private TreeNode root;
    public BTree(String fileName,Type keySchema,int order) {
        PageAllocator allocator = new PageAllocator(fileName, true /* wipe */);

        int headerPageNum = allocator.allocPage();
        assert(headerPageNum == 0);
        this.headerPage = allocator.fetchPage(headerPageNum);
        this.metadata = new BTreeMetadata(allocator, keySchema, order);
        List<Field> keys = new ArrayList<>();
        List<RecordId> rids = new ArrayList<>();
        Optional<Integer> rightSibling = Optional.empty();

        this.root = new LeafNode(metadata, keys,rids, rightSibling);
        writeHeader(headerPage.getByteBuffer());
        headerPage.flush();
    }

    public BTree(String fileName,Type keySchema) {
        this(fileName, keySchema, maxOrder(Page.PAGE_SIZE, keySchema));
    }

    public BTree(String filename) {
        PageAllocator allocator = new PageAllocator(filename, false /* wipe */);
        Page headerPage = allocator.fetchPage(0);
        ByteBuffer buf = headerPage.getByteBuffer();

        Type keySchema = Type.fromBytes(buf);
        int order = buf.getInt();
        int rootPageNum = buf.getInt();

        this.metadata = new BTreeMetadata(allocator, keySchema, order);
        this.headerPage = allocator.fetchPage(0);
        this.root = TreeNode.fromBytes( this.metadata, rootPageNum);
    }

    private void writeHeader(ByteBuffer buf) {
        buf.put(metadata.getKeySchema().toBytes());
        buf.putInt(metadata.getOrder());
        buf.putInt(root.getPage().getPageNum());
    }

    public static int maxOrder(int pageSizeInBytes, Type keySchema) {
        int leafOrder = LeafNode.maxOrder(pageSizeInBytes, keySchema);
        int innerOrder = InnerNode.maxOrder(pageSizeInBytes, keySchema);
        return Math.min(leafOrder, innerOrder);
    }

    public void insert(Field key,RecordId rid) {
        Optional<Pair<Field, Integer>> o = root.insert(key, rid);
        if (!o.isPresent()) {
            return;
        }
        Pair<Field, Integer> p = o.get();

        List<Field> keys = new ArrayList<>();
        keys.add(p.getFirst());

        List<Integer> children = new ArrayList<>();
        children.add(root.getPage().getPageNum());
        children.add(p.getSecond());

        InnerNode inner = new InnerNode(metadata, keys, children);
        this.root = inner;
        writeHeader(headerPage.getByteBuffer());
    }

}
