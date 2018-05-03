package com.github.storage.index;

import com.github.storage.io.Page;
import com.github.storage.table.Field;
import com.github.storage.table.RecordId;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public abstract class TreeNode {
    protected BTreeMetadata metadata;
    protected List<Field> keys;

    public abstract Page getPage();
    public abstract byte[] toBytes();
    public abstract Optional<Pair<Field,Integer>> insert(Field key, RecordId rid);

    protected TreeNode(BTreeMetadata metadata, List<Field> keys) {
        this.keys = keys;
        this.metadata = metadata;
    }
    public static TreeNode fromBytes(BTreeMetadata metadata, int pageNum) {
        Page p = metadata.getAllocator().fetchPage(pageNum);
        ByteBuffer buf = p.getByteBuffer();
        byte b = buf.get();
        if (b == NodeType.LEAF.ordinal()) {
            return LeafNode.fromBytes(metadata, pageNum);
        } else if (b == NodeType.INNER.ordinal()) {
            return InnerNode.fromBytes(metadata, pageNum);
        } else {
            String msg = String.format("Unexpected byte %b.", b);
            throw new IllegalArgumentException(msg);
        }
    }

    public static <T extends Comparable<T>> int  binarySearch(T x, List<T> ys) {
        int lo = 0, hi = ys.size() - 1, mid ;
        while(lo <= hi) {
            mid=lo+(hi-lo)/2;
            int cmp=x.compareTo(ys.get(mid));
            if(cmp<0){hi=mid-1;}
            else if(cmp>0){lo=mid+1;}
            else{return mid;}
        }
        return lo;
    }



    public static void main(String[] args) {
        List<Integer> values = new ArrayList();
        for(int i = 0;i < 512; i+=2) {
            values.add(i);
            System.out.print(i + " ");
        }
        System.out.println();

        int index = binarySearch(5, values);
        System.out.println(index);
    }

}
