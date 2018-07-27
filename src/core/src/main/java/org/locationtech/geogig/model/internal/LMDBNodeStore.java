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
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;
import org.locationtech.geogig.model.internal.ByteArrayOutputStreamPool.InternalByteArrayOutputStream;

import com.google.common.base.Charsets;

import lombok.AllArgsConstructor;
import lombok.NonNull;

@AllArgsConstructor
class LMDBNodeStore {

    private @NonNull Env<ByteBuffer> env;

    private @NonNull Dbi<ByteBuffer> db;

    private @NonNull DirectByteBufferPool keybuffers;

    private @NonNull DirectByteBufferPool valuebuffers;

    private @NonNull ByteArrayOutputStreamPool streams;

    private final CharArrayPool chars = new CharArrayPool();

    private final int dirtyThreshold = 100_000;

    private final Map<String, DAGNode> dirty = new ConcurrentHashMap<>();

    public void close() {
        env = null;
        db = null;
    }

    public DAGNode get(NodeId nodeId) {
        ByteBuffer value;
        try (Txn<ByteBuffer> tx = env.txnRead()) {
            value = db.get(tx, toKey(nodeId.name()));
            if (value == null) {
                throw new NoSuchElementException("Node " + nodeId + " not found");
            }
        }
        DAGNode node = decode(value);
        return node;
    }

    public List<DAGNode> getAll(Set<NodeId> nodeIds) {
        if (nodeIds.isEmpty()) {
            return Collections.emptyList();
        }

        List<DAGNode> res = new ArrayList<>();
        try (Txn<ByteBuffer> tx = env.txnRead()) {
            for (NodeId id : nodeIds) {
                DAGNode node = dirty.get(id.name());
                if (node == null) {
                    ByteBuffer value = db.get(tx, toKey(id.name()));
                    if (value == null) {
                        throw new NoSuchElementException("Node " + id + " not found");
                    }
                    node = decode(value);
                }
                res.add(node);
            }
        }
        return res;
    }

    public void put(NodeId nodeId, DAGNode node) {
        dirty.put(nodeId.name(), node);
        if (dirty.size() >= dirtyThreshold) {
            flush();
        }
    }

    private synchronized void flush() {
        Map<String, DAGNode> save = new HashMap<>(dirty);
        dirty.clear();

        try (Txn<ByteBuffer> tx = env.txnWrite()) {
            for (Map.Entry<String, DAGNode> e : save.entrySet()) {
                String nodeId = e.getKey();
                DAGNode dagNode = e.getValue();
                ByteBuffer key = toKey(nodeId);

                ByteBuffer value = encode(dagNode);
                db.put(tx, key, value);
            }
            tx.commit();
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw e;
        }
    }

    public synchronized void putAll(Map<NodeId, DAGNode> map) {
        map.forEach((id, node) -> dirty.put(id.name(), node));
        if (dirty.size() >= dirtyThreshold) {
            flush();
        }
    }

    private ByteBuffer toKey(String name) {
        CharsetEncoder encoder = Charsets.UTF_8.newEncoder();
        char[] buff = chars.get(name.length());
        name.getChars(0, name.length(), buff, 0);
        CharBuffer in = CharBuffer.wrap(buff, 0, name.length());

        ByteBuffer out = ByteBuffer.wrap(streams.get(2 * name.length()));
        encoder.encode(in, out, true);
        int encodedSize = out.position();

        ByteBuffer key = keybuffers.get().ensureCapacity(encodedSize);
        out.flip();
        key.put(out);
        key.flip();
        return key;
    }

    private ByteBuffer encode(DAGNode node) {
        InternalByteArrayOutputStream raw = rawEncode(node);
        ByteBuffer buff = valuebuffers.get().ensureCapacity(raw.size());
        buff.put(raw.intenal(), 0, raw.size()).flip();
        return buff;
    }

    private InternalByteArrayOutputStream rawEncode(DAGNode node) {
        InternalByteArrayOutputStream buff = streams.get();
        DataOutputStream out = new DataOutputStream(buff);
        try {
            DAGNode.encode(node, out);
            out.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return buff;
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
