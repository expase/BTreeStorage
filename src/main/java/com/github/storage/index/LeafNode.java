package com.github.storage.index;

import com.github.storage.io.Page;
import com.github.storage.table.Field;
import com.github.storage.table.RecordId;
import com.github.storage.table.Type;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

public class LeafNode extends TreeNode {


    private List<RecordId> rids;
    private Optional<Integer> rightSibling;
    private Page page;

    public LeafNode(BTreeMetadata metadata, List<Field> keys,
                    List<RecordId> rids, Optional<Integer> rightSibling) {
        this(metadata, metadata.getAllocator().allocPage(), keys, rids,
                rightSibling);
    }

    @Override
    public Page getPage() {
        return page;
    }

    private LeafNode(BTreeMetadata metadata, int pageNum, List<Field> keys,
                     List<RecordId> rids, Optional<Integer> rightSibling) {
        super(metadata, keys);
        assert(keys.size() <= 2 * metadata.getOrder());
        assert(keys.size() == rids.size());


        this.page = metadata.getAllocator().fetchPage(pageNum);

        this.rids = rids;
        this.rightSibling = rightSibling;
        sync();
    }

    private void sync() {
        page.getByteBuffer().put(toBytes());
        page.flush();
    }

    public static int maxOrder(int pageSizeInBytes, Type keySchema) {

        int keySize = keySchema.getSizeInBytes();
        int ridSize = RecordId.getSizeInBytes();
        int n = (pageSizeInBytes - 9) / (keySize + ridSize);
        return n / 2;
    }


    public byte[] toBytes() {

        int isLeafSize = 1;
        int siblingSize = Integer.BYTES;
        int lenSize = Integer.BYTES;
        int keySize = metadata.getKeySchema().getSizeInBytes();
        int ridSize = RecordId.getSizeInBytes();
        int entriesSize = (keySize + ridSize) * keys.size();
        int size = isLeafSize + siblingSize + lenSize + entriesSize;

        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.put((byte) 1);
        buf.putInt(rightSibling.orElse(-1));
        buf.putInt(keys.size());
        for (int i = 0; i < keys.size(); ++i) {
            buf.put(keys.get(i).toBytes());
            buf.put(rids.get(i).toBytes());
        }
        return buf.array();
    }

    @Override
    public Optional<Pair<Field,Integer>> insert(Field key, RecordId rid) {
        int index = binarySearch(key, keys);
        keys.add(index, key);
        rids.add(index, rid);
        int d = metadata.getOrder();
        if (keys.size() <= 2 * d) { //don't split
            sync();
            return Optional.empty();
        }

        //need split
        assert(keys.size() == 2*d + 1);
        List<Field> leftKeys = keys.subList(0, d);
        List<Field> rightKeys = keys.subList(d, 2*d + 1);
        List<RecordId> leftRids  = rids.subList(0, d);
        List<RecordId> rightRids  = rids.subList(d, 2*d + 1);

        LeafNode rightNode = new LeafNode(metadata, rightKeys, rightRids, rightSibling);
        int pageNum = rightNode.getPage().getPageNum();
        this.keys = leftKeys;
        this.rids = leftRids;
        this.rightSibling = Optional.of(pageNum);
        sync();

        return Optional.of(new Pair<>(rightKeys.get(0), pageNum));
    }




}
