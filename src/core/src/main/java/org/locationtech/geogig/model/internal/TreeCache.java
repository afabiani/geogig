/* Copyright (c) 2015-2016 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.model.internal;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.RevTree;
import org.locationtech.geogig.storage.BulkOpListener;
import org.locationtech.geogig.storage.ObjectStore;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

class TreeCache {
    private final LoadingCache<ObjectId, RevTree> cache;

    private final ObjectStore store;

    public TreeCache(final ObjectStore store) {
        this.store = store;

        final CacheLoader<ObjectId, RevTree> loader = new CacheLoader<ObjectId, RevTree>() {

            public @Override RevTree load(ObjectId key) throws Exception {
                RevTree tree = TreeCache.this.store.getTree(key);
                return tree;
            }
        };
        this.cache = CacheBuilder.newBuilder().concurrencyLevel(1).maximumSize(100_000)
                .build(loader);
    }

    public RevTree getTree(final ObjectId treeId) {
        final RevTree tree = resolve(treeId);
        if (tree.bucketsSize() > 0) {
            List<ObjectId> bucketIds = new ArrayList<>(tree.bucketsSize());
            tree.forEachBucket((i, b) -> bucketIds.add(b.getObjectId()));
            preload(bucketIds);
        }
        return tree;
    }

    public RevTree resolve(final ObjectId leafRevTreeId) {
        RevTree tree = cache.getUnchecked(leafRevTreeId);
        Preconditions.checkNotNull(tree);
        return tree;
    }

    public void preload(Iterable<ObjectId> trees) {
        Iterator<RevTree> preloaded = store.getAll(trees, BulkOpListener.NOOP_LISTENER,
                RevTree.class);
        ConcurrentMap<ObjectId, RevTree> map = cache.asMap();
        preloaded.forEachRemaining(t -> map.putIfAbsent(t.getId(), t));
    }
}