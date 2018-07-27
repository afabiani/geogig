package org.locationtech.geogig.model.internal;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class DirectByteBufferPool {

    public static class Buffer {

        public ByteBuffer buf;

        private final int pageSize;

        private Buffer(int pageSize) {
            this.pageSize = pageSize;
        }

        public ByteBuffer ensureCapacity(int size) {
            if (buf == null || buf.capacity() < size) {
                int pages = 2 * (1 + size / pageSize);
                this.buf = ByteBuffer.allocateDirect(pages * pageSize);
            }
            buf.position(0).limit(buf.capacity());
            return buf;
        }
    }

    private Map<Thread, Buffer> pool = new ConcurrentHashMap<>();

    private int pageSize;

    public DirectByteBufferPool() {
        this(4096);
    }

    public DirectByteBufferPool(int pageSize) {
        this.pageSize = pageSize;
    }

    public Buffer get() {
        return pool.computeIfAbsent(Thread.currentThread(), t -> new Buffer(pageSize));
    }

    public void dispose() {
        pool.clear();
    }
}
