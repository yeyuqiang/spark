package org.apache.spark.pmem;

import org.apache.spark.pmem.util.NativeLibraryLoader;
import sun.misc.Cleaner;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

public class PersistentMemoryPlatform {
    public static final Object lock = new Object();
    private static boolean initialized = false;

    static {
        NativeLibraryLoader.load("pmplatform");
    }

    public static void initialize(String initializePath, long initializeSize) {
        if (!initialized) {
            synchronized (lock) {
                if (!initialized) {
                    initializeNative(initializePath, initializeSize);
                    initialized = true;
                }
            }
        }
    }

    private static native void initializeNative(String initializePath, long initializeSize);

    public static native long allocateMemory(long size);

    public static ByteBuffer allocateDirectBuffer(int size) {
        try {
            Class<?> cls = Class.forName("java.nio.DirectByteBuffer");
            Constructor<?> constructor = cls.getDeclaredConstructor(Long.TYPE, Integer.TYPE);
            constructor.setAccessible(true);
            Field cleanerField = cls.getDeclaredField("cleaner");
            cleanerField.setAccessible(true);
            final long memory = allocateMemory(size);
            ByteBuffer buffer = (ByteBuffer) constructor.newInstance(memory, size);
            Cleaner cleaner = Cleaner.create(buffer, new Runnable() {
                @Override
                public void run() {
                    freeMemory(memory);
                }
            });
            cleanerField.set(buffer, cleaner);
            return buffer;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static native void freeMemory(long address);

    public static native void copyMemory(long destAddress, long srcAddress, long size);
}