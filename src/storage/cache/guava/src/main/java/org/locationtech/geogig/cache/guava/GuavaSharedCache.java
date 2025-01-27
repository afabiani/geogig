/* Copyright (c) 2018 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan - Pulled off from SharedCache.Impl as a top level class
 */
package org.locationtech.geogig.cache.guava;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.jdt.annotation.Nullable;
import org.locationtech.geogig.flatbuffers.FlatBuffersRevObjectSerializer;
import org.locationtech.geogig.model.RevObject;
import org.locationtech.geogig.model.RevObject.TYPE;
import org.locationtech.geogig.model.RevTree;
import org.locationtech.geogig.storage.RevObjectSerializer;
import org.locationtech.geogig.storage.cache.CacheIdentifier;
import org.locationtech.geogig.storage.cache.CacheKey;
import org.locationtech.geogig.storage.cache.CacheStats;
import org.locationtech.geogig.storage.cache.SharedCache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class GuavaSharedCache implements SharedCache {
    /**
     * Executor service used to encode a {@link RevObject} to a {@code byte[]} and add it to the
     * L2cache.
     * <p>
     * The executor alleviates the overhead of adding an object to the cache, as it needs to be
     * serialized, but uses a bounded queue of up to # of cores, and a {@link CallerRunsPolicy} as
     * rejected execution handler, so that under load, the calling thread will pay the price of
     * encoding and caching instead of running an unbounded number of threads to store the objects
     * in the cache.
     * 
     * @see #insert(CacheKey, RevObject)
     */
    static final ExecutorService WRITE_BACK_EXECUTOR;
    static {
        ThreadFactory tf = new ThreadFactoryBuilder().setDaemon(true)
                .setNameFormat("GeoGig shared cache %d").build();

        final int nThreads = Math.max(4, Runtime.getRuntime().availableProcessors() / 2);

        RejectedExecutionHandler sameThreadHandler = new ThreadPoolExecutor.CallerRunsPolicy();

        int corePoolSize = 1;
        WRITE_BACK_EXECUTOR = new ThreadPoolExecutor(corePoolSize, nThreads, 30L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(nThreads), tf, sameThreadHandler);
    }

    private static final RevObjectSerializer ENCODER = new FlatBuffersRevObjectSerializer();

    private RevObjectSerializer encoder = ENCODER;

    /**
     * Used to track the size in bytes of the cache, since {@link Cache} can return only the
     * approximate number of entries but not the accumulated {@link Weigher#weigh weight}
     *
     */
    private static class SizeTracker implements RemovalListener<CacheKey, byte[]> {

        private static Weigher<CacheKey, byte[]> WEIGHER = new Weigher<CacheKey, byte[]>() {

            static final int ESTIMATED_Key_SIZE = 32;

            public @Override int weigh(CacheKey key, byte[] value) {
                return ESTIMATED_Key_SIZE + value.length;
            }

        };

        public final AtomicLong size = new AtomicLong();

        public @Override void onRemoval(RemovalNotification<CacheKey, byte[]> notification) {
            CacheKey key = notification.getKey();
            byte[] value = notification.getValue();
            int weigh = WEIGHER.weigh(key, value);
            size.addAndGet(-weigh);
        }

        public void inserted(CacheKey id, byte[] value) {
            int weigh = WEIGHER.weigh(id, value);
            size.addAndGet(weigh);
        }
    }

    public void setEncoder(RevObjectSerializer encoder) {
        this.encoder = encoder;
    }

    /**
     * The Level1 cache contains most recently used, already parsed, instances of {@link RevTree}
     * objects as they tend to be slow to parse and queried very often.
     * <p>
     * When trees are evicted from the L1Cache due to size constraints, their serialized version
     * will be added to the L2Cache if it's not already present.
     * 
     * @see #put(CacheKey, RevObject)
     * @see #getIfPresent(CacheKey)
     */
    final Cache<CacheKey, RevTree> L1Cache;

    /**
     * The Level2 cache contains serialized versions of RevObjects, as they take less memory than
     * Java objects and their size can be more or less accurately tracked.
     */
    final Cache<CacheKey, byte[]> L2Cache;

    private final GuavaSharedCache.SizeTracker sizeTracker;

    private long maxCacheSizeBytes;

    GuavaSharedCache() {
        this.L1Cache = CacheBuilder.newBuilder().maximumSize(0).build();
        this.L2Cache = CacheBuilder.newBuilder().maximumSize(0).build();
        this.sizeTracker = new SizeTracker();
    }

    public GuavaSharedCache(final int l1capacity, final long maxCacheSizeBytes) {
        this.maxCacheSizeBytes = maxCacheSizeBytes;
        checkArgument(l1capacity >= 0);
        checkArgument(maxCacheSizeBytes >= 0, "Cache size can't be < 0, 0 meaning no cache at all");
        int initialCapacityCount = 1_000_000;
        int concurrencyLevel = 16;

        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        cacheBuilder = cacheBuilder.maximumWeight(maxCacheSizeBytes);
        cacheBuilder.weigher(GuavaSharedCache.SizeTracker.WEIGHER);

        cacheBuilder.initialCapacity(initialCapacityCount);
        cacheBuilder.concurrencyLevel(concurrencyLevel);
        cacheBuilder.recordStats();

        this.sizeTracker = new SizeTracker();
        cacheBuilder.removalListener(sizeTracker);

        RemovalListener<CacheKey, RevObject> L1WriteBack = (notification) -> {
            RemovalCause cause = notification.getCause();
            if (RemovalCause.SIZE == cause) {
                CacheKey key = notification.getKey();
                RevObject value = notification.getValue();
                if (value != null) {
                    putInternal(key, value);
                }
            }
        };

        this.L1Cache = CacheBuilder.newBuilder()//
                .concurrencyLevel(1)//
                .maximumSize(l1capacity)//
                .softValues()//
                .removalListener(L1WriteBack)//
                .build();
        this.L2Cache = cacheBuilder.build();
    }

    public @Override boolean contains(CacheKey id) {
        boolean contains = L1Cache.asMap().containsKey(id) || L2Cache.asMap().containsKey(id);
        return contains;
    }

    public @Override void invalidateAll() {
        L1Cache.invalidateAll();
        L2Cache.invalidateAll();

        L1Cache.cleanUp();
        L2Cache.cleanUp();
    }

    public @Override void invalidateAll(CacheIdentifier prefix) {
        invalidateAll(prefix, L1Cache.asMap());
        invalidateAll(prefix, L2Cache.asMap());
    }

    private void invalidateAll(CacheIdentifier prefix, ConcurrentMap<CacheKey, ?> map) {
        map.keySet().parallelStream().filter((k) -> {
            int keyprefix = k.prefix();
            int expectedPrefix = prefix.prefix();
            return keyprefix == expectedPrefix;
        }).forEach(map::remove);
    }

    public @Override void dispose() {
        invalidateAll();
    }

    public @Override void invalidate(CacheKey id) {
        L2Cache.invalidate(id);
    }

    /**
     * Returns the cached {@link RevObject}, if present in either the L1 or L2 cache, or
     * {@code null} otherwise.
     * <p>
     * As {@link RevTree}s are frequently requested and tend to be slower to parse, cache miss to
     * the L1 cache that resulted in a cache hit to the L2 cache, and where the resulting object is
     * a {@code RevTree}, will result in the tree being added back to the L1 cache.
     */
    public @Override @Nullable RevObject getIfPresent(CacheKey key) {
        RevObject obj = L1Cache.getIfPresent(key);
        if (obj == null) {
            // call cache.getIfPresent instead of map.get() or the cache stats don't record the
            // hits/misses
            byte[] val = L2Cache.getIfPresent(key);
            if (val != null) {
                obj = decode(key, val);
                if (TYPE.TREE == obj.getType()) {// keep L1 hot on tree objects
                    L1Cache.asMap().putIfAbsent(key, (RevTree) obj);
                }
            }
        }
        return obj;
    }

    /**
     * Adds the given object to the cache under the given key, if not already present.
     * <p>
     * If the object happens to be a {@link RevTree}, it will first be added to the
     * {@link #L1Cache}. In either case, it's serialized version will be, possibly asynchronously,
     * added to the {@link #L2Cache}.
     */
    public @Override @Nullable Future<?> put(CacheKey key, RevObject obj) {
        RevObject l1val = TYPE.TREE == obj.getType()
                ? L1Cache.asMap().putIfAbsent(key, (RevTree) obj)
                : null;
        if (maxCacheSizeBytes > 0L && l1val == null) {
            // add it to L2 if not already present, even if it's a RevTree and has been added to
            // the L1 cache, since removal notifications happen after the fact
            return putInternal(key, obj);
        }
        return null;
        // insert(key, obj);
        // return null;
    }

    @Nullable
    Future<?> putInternal(CacheKey key, RevObject obj) {
        if (L2Cache.asMap().containsKey(key)) {
            return null;
        }
        return WRITE_BACK_EXECUTOR.submit(() -> insert(key, obj));
    }

    void insert(CacheKey key, RevObject obj) {
        byte[] value = encode(obj);
        if (null == L2Cache.asMap().putIfAbsent(key, value)) {
            sizeTracker.inserted(key, value);
        }
    }

    private byte[] encode(RevObject obj) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            encoder.write(obj, out);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        byte[] byteArray = out.toByteArray();
        return byteArray;
    }

    private RevObject decode(CacheKey key, byte[] val) {
        try {
            return encoder.read(key.id(), val, 0, val.length);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public @Override String toString() {
        long size = L2Cache.size();
        long bytes = sizeTracker.size.get();
        long avg = size == 0 ? 0 : bytes / size;
        return String.format("Size: %,d, bytes: %,d, avg: %,d bytes/entry, %s", size, bytes, avg,
                L2Cache.stats());
    }

    public @Override long sizeBytes() {
        return sizeTracker.size.get();
    }

    public @Override long objectCount() {
        return L2Cache.size();
    }

    public @Override CacheStats getStats() {
        final com.google.common.cache.CacheStats stats = L2Cache.stats();
        return new CacheStats() {
            public @Override long hitCount() {
                return stats.hitCount();
            }

            public @Override double hitRate() {
                return stats.hitRate();
            }

            public @Override long missCount() {
                return stats.missCount();
            }

            public @Override double missRate() {
                return stats.missRate();
            }

            public @Override long evictionCount() {
                return stats.evictionCount();
            }
        };
    }
}