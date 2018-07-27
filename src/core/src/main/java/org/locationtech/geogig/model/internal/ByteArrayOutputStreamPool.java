package org.locationtech.geogig.model.internal;

import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class ByteArrayOutputStreamPool {

    static final class InternalByteArrayOutputStream extends ByteArrayOutputStream {
        InternalByteArrayOutputStream() {
            super(1024);
        }

        InternalByteArrayOutputStream(int initialCapacity) {
            super(initialCapacity);
        }

        public byte[] intenal() {
            return super.buf;
        }

        void ensureCapacity(int minimumCapacity) {
            if (super.buf.length < minimumCapacity) {
                super.buf = new byte[2 * minimumCapacity];
            }
        }
    }

    private Map<Thread, InternalByteArrayOutputStream> pool = new ConcurrentHashMap<>();

    private int defaultCapacity;

    public ByteArrayOutputStreamPool() {
        this(4096);
    }

    public ByteArrayOutputStreamPool(int pageSize) {
        this.defaultCapacity = pageSize;
    }

    public byte[] get(int minimumCapacity) {
        InternalByteArrayOutputStream buff = get();
        buff.ensureCapacity(minimumCapacity);
        return buff.intenal();
    }

    public InternalByteArrayOutputStream get() {
        InternalByteArrayOutputStream stream = pool.computeIfAbsent(Thread.currentThread(),
                t -> new InternalByteArrayOutputStream(defaultCapacity));
        stream.reset();
        return stream;
    }

    public void dispose() {
        pool.clear();
    }
}
