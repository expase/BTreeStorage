package com.github.storage.index;

import com.github.storage.io.PageAllocator;
import com.github.storage.table.Type;

public class BTreeMetadata {
    private final PageAllocator allocator;

    private final Type keySchema;

    private final int order;

    public BTreeMetadata(PageAllocator allocator, Type keySchema, int order) {
        this.allocator = allocator;
        this.keySchema = keySchema;
        this.order = order;
    }

    public PageAllocator getAllocator() {
        return allocator;
    }

    public Type getKeySchema() {
        return keySchema;
    }

    public int getOrder() {
        return order;
    }
}
