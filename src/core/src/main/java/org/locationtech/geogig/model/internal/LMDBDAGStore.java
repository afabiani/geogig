/* Copyright (c) 2016 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan - initial implementation
 */
package org.locationtech.geogig.model.internal;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.eclipse.jdt.annotation.Nullable;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;
import org.locationtech.geogig.model.ObjectId;

import com.google.common.collect.Maps;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import lombok.AllArgsConstructor;
import lombok.NonNull;

@AllArgsConstructor
class LMDBDAGStore {

    private @NonNull Env<ByteBuffer> env;

    private @NonNull Dbi<ByteBuffer> db;

    private @NonNull DirectByteBufferPool keybuffers, valuebuffers;

    private final int dirtyThreshold = 10_000;

    private final Map<TreeId, DAG> dirty = new ConcurrentHashMap<>();

    public void close() {
        env = null;
        db = null;
    }

    public DAG getOrCreate(final TreeId treeId, final ObjectId originalTreeId) {
        ByteBuffer key = toKey(treeId);
        DAG dag;
        dag = getInternal(treeId, key);
        if (dag == null) {
            dag = new DAG(treeId, originalTreeId);
            putInternal(key, dag);
        }
        return dag;
    }

    private @Nullable DAG getInternal(TreeId id, final ByteBuffer key) {
        DAG dag = dirty.get(id);
        if (dag == null) {
            try (Txn<ByteBuffer> t = env.txnRead()) {
                ByteBuffer value = db.get(t, key);
                if (null != value) {
                    dag = decode(id, value);
                }
            }
        }
        return dag;
    }

    public List<DAG> getTrees(final Set<TreeId> ids, List<DAG> target)
            throws NoSuchElementException {

        try (Txn<ByteBuffer> t = env.txnRead()) {
            for (TreeId id : ids) {
                DAG dag = dirty.get(id);
                if (dag == null) {
                    ByteBuffer val = db.get(t, toKey(id));
                    if (val == null) {
                        throw new NoSuchElementException(id + " not found");
                    }
                    dag = decode(id, val);
                }
                target.add(dag);
            }
        }
        return target;
    }

    public void putAll(Map<TreeId, DAG> dags) {
        Map<TreeId, DAG> changed = Maps.filterValues(dags, (d) -> d.isMutated());
        dirty.putAll(changed);
        if (dirty.size() >= dirtyThreshold) {
            flush();
        }
    }

    private synchronized void flush() {
        List<DAG> save = new ArrayList<>(dirty.values());
        dirty.clear();

        try (Txn<ByteBuffer> t = env.txnWrite()) {
            for (DAG d : save) {
                ByteBuffer key = toKey(d.getId());
                ByteBuffer val = encode(d);
                db.put(t, key, val);
            }
            t.commit();
        }

    }

    private void putInternal(ByteBuffer key, DAG dag) {
        dirty.put(dag.getId(), dag);
        if (dirty.size() >= dirtyThreshold) {
            flush();
        }
    }

    private ByteBuffer toKey(TreeId treeId) {
        byte[] raw = treeId.bucketIndicesByDepth;
        ByteBuffer key = keybuffers.get().ensureCapacity(raw.length);
        key.put(raw).flip();
        return key;
    }

    private DAG decode(TreeId id, ByteBuffer value) {
        DAG dag;
        try {
            dag = DAG.deserialize(id, new DataInputStream(new ByteBufferInputStream(value)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return dag;
    }

    private ByteBuffer encode(DAG bucketDAG) {
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        try {
            DAG.serialize(bucketDAG, out);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        byte[] raw = out.toByteArray();
        ByteBuffer buff = valuebuffers.get().ensureCapacity(raw.length);
        buff.put(raw).flip();
        return buff;
    }
}
