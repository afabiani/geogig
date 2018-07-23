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

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;

import lombok.AllArgsConstructor;
import lombok.NonNull;

@AllArgsConstructor
class LMDBNodeStore {

    private @NonNull Env<ByteBuffer> env;

    private @NonNull Dbi<ByteBuffer> db;

    private @NonNull DirectByteBufferPool keybuffers;

    private @NonNull DirectByteBufferPool valuebuffers;

    private final Map<NodeId, DAGNode> dirty = new ConcurrentHashMap<>();

    private final ExecutorService storeExecutor = Executors.newFixedThreadPool(16);

    public void close() {
        env = null;
        db = null;
        storeExecutor.shutdownNow();
    }

    public DAGNode get(NodeId nodeId) {
        DAGNode node = dirty.get(nodeId);
        if (node == null) {
            ByteBuffer value;
            try (Txn<ByteBuffer> tx = env.txnRead()) {
                value = db.get(tx, toKey(nodeId));
                if (value == null) {
                    throw new NoSuchElementException("Node " + nodeId + " not found");
                }
            }
            node = decode(value);
        }
        return node;
    }

    public Map<NodeId, DAGNode> getAll(Set<NodeId> nodeIds) {
        if (nodeIds.isEmpty()) {
            return ImmutableMap.of();
        }
        storeExecutor.shutdown();
        while (!storeExecutor.isTerminated()) {
            try {
                storeExecutor.awaitTermination(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        Map<NodeId, DAGNode> res = new HashMap<>();
        try (Txn<ByteBuffer> tx = env.txnRead()) {
            for (NodeId id : nodeIds) {
                DAGNode dirtyNode = dirty.get(id);
                if (dirtyNode != null) {
                    res.put(id, dirtyNode);
                    continue;
                }
                ByteBuffer value = db.get(tx, toKey(id));
                if (value == null) {
                    throw new NoSuchElementException("Node " + id + " not found");
                }
                res.put(id, decode(value));
            }
        }
        return res;
    }

    public void put(NodeId nodeId, DAGNode node) {
        dirty.put(nodeId, node);
        if (dirty.size() >= 1e5) {
            flush();
        }
    }

    private void flush() {
        HashMap<NodeId, DAGNode> map = new HashMap<>(dirty);
        dirty.clear();
        storeExecutor.submit(() -> {
            ByteArrayOutputStream out = new ByteArrayOutputStream();

            // Stopwatch sw = Stopwatch.createStarted();
            try (Txn<ByteBuffer> tx = env.txnWrite()) {
                for (Map.Entry<NodeId, DAGNode> e : map.entrySet()) {
                    NodeId nodeId = e.getKey();
                    DAGNode dagNode = e.getValue();
                    ByteBuffer key = toKey(nodeId);

                    out.reset();
                    ByteBuffer value = encode(dagNode, out);
                    db.put(tx, key, value);
                }
                tx.commit();
            } catch (RuntimeException e) {
                e.printStackTrace();
                throw e;
            }
        });
        // System.out.printf("\nflushed %,d nodes in %s\n", map.size(), sw.stop());
    }

    public void putAll(Map<NodeId, DAGNode> nodeMappings) {
        dirty.putAll(nodeMappings);
        if (dirty.size() >= 1e5) {
            flush();
        }
    }

    private ByteBuffer toKey(NodeId nodeId) {
        byte[] raw = nodeId.name().getBytes(Charsets.UTF_8);
        ByteBuffer key = keybuffers.get().ensureCapacity(raw.length);
        key.put(raw).flip();
        return key;
    }

    private ByteBuffer encode(DAGNode node, ByteArrayOutputStream outstream) {
        byte[] raw = rawEncode(node, outstream);
        ByteBuffer buff = valuebuffers.get().ensureCapacity(raw.length);
        buff.put(raw).flip();
        return buff;
    }

    private byte[] rawEncode(DAGNode node, ByteArrayOutputStream outstream) {

        DataOutputStream out = new DataOutputStream(outstream);
        try {
            DAGNode.encode(node, out);
            out.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        byte[] raw = outstream.toByteArray();
        return raw;
    }

    private DAGNode decode(ByteBuffer nodeData) {
        DAGNode node;
        try {
            node = DAGNode.decode(new DataInputStream(new ByteBufferInputStream(nodeData)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return node;
    }
}
