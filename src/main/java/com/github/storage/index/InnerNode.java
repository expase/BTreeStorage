package com.github.storage.index;

import com.github.storage.io.Page;
import com.github.storage.table.Field;
import com.github.storage.table.RecordId;
import com.github.storage.table.Type;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


public class InnerNode extends TreeNode {


    private List<Integer> children;
    private Page page;

    public InnerNode(BTreeMetadata metadata,  List<Field>keys, List<Integer> children) {
        this(metadata, metadata.getAllocator().allocPage(), keys, children);
    }

    private InnerNode(BTreeMetadata metadata, int pageNum, List<Field> keys,
                      List<Integer> children) {
        super(metadata, keys);
        assert(keys.size() <= 2 * metadata.getOrder());
        assert(keys.size() + 1 == children.size());


        this.page = metadata.getAllocator().fetchPage(pageNum);

        this.children = children;
        sync();
    }
    private void sync() {
        page.getByteBuffer().put(toBytes());
        page.flush();
    }
    @Override
    public Page getPage() {
        return page;
    }

    private TreeNode getNode(int i) {
        int pageNum = children.get(i);
        return TreeNode.fromBytes(metadata, pageNum);
    }

    @Override
    public byte[] toBytes() {

        int isLeafSize = 1;
        int numKeysSize = Integer.BYTES;
        int keysSize = metadata.getKeySchema().getSizeInBytes() * keys.size();
        int childrenSize = Integer.BYTES * children.size();
        int size = isLeafSize + numKeysSize + keysSize + childrenSize;

        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.put((byte) 0);
        buf.putInt(keys.size());
        for (Field key : keys) {
            buf.put(key.toBytes());
        }
        for (Integer child : children) {
            buf.putInt(child);
        }
        return buf.array();
    }

    @Override
    public Optional<Pair<Field,Integer>> insert(Field key, RecordId rid) {
        int index = binarySearch(key, keys);
        TreeNode child = getNode(index);

        Optional<Pair<Field, Integer>> o = child.insert(key, rid);
        if(!o.isPresent()) {
            return Optional.empty();
        }

        Pair<Field, Integer> p = o.get();
        keys.add(index, p.getFirst());
        children.add(index + 1, p.getSecond());

        int d = metadata.getOrder();
        if(keys.size()<=2 * d) {
            sync();
            return Optional.empty();
        }

        List<Field> leftKeys = keys.subList(0, d);
        Field middle = keys.get(d);

        List<Field> rightKeys = keys.subList(d + 1, 2*d + 1);
        List<Integer> leftChildren = children.subList(0, d + 1);
        List<Integer> rightChildren = children.subList(d + 1, 2*d + 2);

        InnerNode n = new InnerNode(metadata, rightKeys, rightChildren);
        this.keys = leftKeys;
        this.children = leftChildren;
        sync();
        return Optional.of(new Pair<>(middle, n.getPage().getPageNum()));

    }

    public static InnerNode fromBytes(BTreeMetadata metadata, int pageNum) {
        Page page = metadata.getAllocator().fetchPage(pageNum);
        ByteBuffer buf = page.getByteBuffer();

        assert(buf.get() == (byte) 0);

        List<Field> keys = new ArrayList<>();
        List<Integer> children = new ArrayList<>();
        int n = buf.getInt();
        for (int i = 0; i < n; ++i) {
            keys.add(Field.fromBytes(buf, metadata.getKeySchema()));
        }
        for (int i = 0; i < n + 1; ++i) {
            children.add(buf.getInt());
        }
        return new InnerNode(metadata, pageNum, keys, children);
    }

    public static int maxOrder(int pageSizeInBytes, Type keySchema) {
        // A leaf node with n entries takes up the following number of bytes:
        //
        //   1 + 4 + (n * keySize) + ((n + 1) * 4)
        //
        // where
        //
        //   - 1 is the number of bytes used to store isLeaf,
        //   - 4 is the number of bytes used to store n,
        //   - keySize is the number of bytes used to store a DataBox of type
        //     keySchema, and
        //   - 4 is the number of bytes used to store a child pointer.
        //

        int keySize = keySchema.getSizeInBytes();
        int n = (pageSizeInBytes - 9) / (keySize + 4);
        return n / 2;
    }

}
