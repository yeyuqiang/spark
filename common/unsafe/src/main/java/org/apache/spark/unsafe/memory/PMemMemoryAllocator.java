package org.apache.spark.unsafe.memory;

import com.intel.oap.common.unsafe.PersistentMemoryPlatform;

public class PMemMemoryAllocator implements MemoryAllocator {

    @Override
    public MemoryBlock allocate(long size) throws OutOfMemoryError {
        long address = PersistentMemoryPlatform.allocateVolatileMemory(size);
        MemoryBlock memoryBlock = new MemoryBlock(null, address, size);
        return memoryBlock;
    }

    @Override
    public void free(MemoryBlock memory) {
        assert (memory.getBaseObject() == null) :
                "Fail to free the memory block by PMem Memory Allocator";
        PersistentMemoryPlatform.freeMemory(memory.getBaseOffset());
    }
}
