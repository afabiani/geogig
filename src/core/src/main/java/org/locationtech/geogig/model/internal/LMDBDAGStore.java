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
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import org.eclipse.jdt.annotation.Nullable;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;
import org.locationtech.geogig.model.ObjectId;

import com.google.common.base.Preconditions;
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
        DAG dag = null;
        try (Txn<ByteBuffer> t = env.txnRead()) {
            ByteBuffer value = db.get(t, key);
            if (null != value) {
                dag = decode(id, value);
            }
        }
        return dag;
    }

    public List<DAG> getTrees(final Set<TreeId> ids) throws NoSuchElementException {

        List<DAG> dags = new ArrayList<>(ids.size());

        try (Txn<ByteBuffer> t = env.txnRead()) {
            for (TreeId id : ids) {
                ByteBuffer val = db.get(t, toKey(id));
                Preconditions.checkState(val != null);
                DAG dag = decode(id, val);
                dags.add(dag);
            }
        }
        return dags;
    }

    public void putAll(Map<TreeId, DAG> dags) {
        Map<TreeId, DAG> changed = Maps.filterValues(dags, (d) -> d.isMutated());

        try (Txn<ByteBuffer> t = env.txnWrite()) {
            for (Entry<TreeId, DAG> e : changed.entrySet()) {
                ByteBuffer key = toKey(e.getKey());
                ByteBuffer val = encode(e.getValue());
                db.put(t, key, val);
            }
            t.commit();
        }
    }

    private void putInternal(ByteBuffer key, DAG dag) {
        ByteBuffer value = encode(dag);
        try (Txn<ByteBuffer> t = env.txnWrite()) {
            db.put(t, key, value);
            t.commit();
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
