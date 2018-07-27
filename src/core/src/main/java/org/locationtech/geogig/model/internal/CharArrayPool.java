package org.locationtech.geogig.model.internal;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class CharArrayPool {

    private Map<Thread, char[]> pool = new ConcurrentHashMap<>();

    private int defaultCapacity;

    public CharArrayPool() {
        this(128);
    }

    public CharArrayPool(int pageSize) {
        this.defaultCapacity = pageSize;
    }

    public char[] get(int minimumCapacity) {
        char[] buff = pool.computeIfAbsent(Thread.currentThread(),
                t -> new char[Math.max(minimumCapacity, defaultCapacity)]);
        if (buff.length < minimumCapacity) {
            buff = new char[2 * minimumCapacity];
            pool.put(Thread.currentThread(), buff);
        }
        return buff;
    }

    public void dispose() {
        pool.clear();
    }
}
