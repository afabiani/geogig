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

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.eclipse.jdt.annotation.Nullable;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.locationtech.geogig.model.Node;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.RevTree;
import org.locationtech.geogig.storage.ObjectStore;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

class LMDBDAGStorageProvider implements DAGStorageProvider {

    private final ObjectStore objectStore;

    private final TreeCache treeCache;

    private Env<ByteBuffer> dbEnv;

    private File dagDbDir = null;

    private Dbi<ByteBuffer> dagDb, nodeDb;

    private LMDBNodeStore nodeStore;

    private LMDBDAGStore dagStore;

    private DirectByteBufferPool keybuffers = new DirectByteBufferPool(128);

    private DirectByteBufferPool valuebuffers = new DirectByteBufferPool(4096);

    LMDBDAGStorageProvider(ObjectStore source) {
        this(source, new TreeCache(source));
    }

    LMDBDAGStorageProvider(ObjectStore source, TreeCache treeCache) {
        this.objectStore = source;
        this.treeCache = treeCache;
        try {
            dagDbDir = Files.createTempDirectory("geogig-dag-store").toFile();
            dbEnv = Env.create()//
                    .setMapSize(1 * 1024L * 1024 * 1024 * 1024)//
                    .setMaxDbs(2)//
                    .setMaxReaders(128)//
                    .open(dagDbDir, //
                            EnvFlags.MDB_WRITEMAP, //
                            EnvFlags.MDB_MAPASYNC, //
                            EnvFlags.MDB_NOMETASYNC, //
                            EnvFlags.MDB_NOSYNC);

            dagDb = dbEnv.openDbi("dag-store", DbiFlags.MDB_CREATE);
            nodeDb = dbEnv.openDbi("node-store", DbiFlags.MDB_CREATE);

            this.dagStore = new LMDBDAGStore(dbEnv, dagDb, keybuffers, valuebuffers);
            this.nodeStore = new LMDBNodeStore(dbEnv, nodeDb, keybuffers, valuebuffers);
        } catch (Exception e) {
            dispose();
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    public @Override void dispose() {
        try {
            if (null != dagStore) {
                dagStore.close();
                dagStore = null;
            }
        } finally {
            try {
                if (null != nodeStore) {
                    nodeStore.close();
                    nodeStore = null;
                }
            } finally {
                try {
                    if (null != dbEnv) {
                        dbEnv.close();
                        dbEnv = null;
                    }
                } finally {
                    keybuffers.dispose();
                    valuebuffers.dispose();
                    delete(dagDbDir);
                }
            }
        }
    }

    private void delete(@Nullable File file) {
        if (null == file || !file.exists()) {
            return;
        }
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File f : children) {
                    delete(f);
                }
            }
        }
        file.delete();
    }

    public @Override TreeCache getTreeCache() {
        return treeCache;
    }

    public @Override List<DAG> getTrees(Set<TreeId> ids) throws NoSuchElementException {
        return dagStore.getTrees(ids);
    }

    public @Override DAG getOrCreateTree(TreeId treeId, ObjectId originalTreeId) {
        return dagStore.getOrCreate(treeId, originalTreeId);
    }

    public @Override void save(Map<TreeId, DAG> dags) {
        dagStore.putAll(dags);
    }

    public @Override Map<NodeId, Node> getNodes(final Set<NodeId> nodeIds) {
        Map<NodeId, DAGNode> dagNodes = nodeStore.getAll(nodeIds);
        return Maps.transformValues(dagNodes, (dn) -> dn.resolve(treeCache));
    }

    public @Override void saveNode(NodeId nodeId, Node node) {
        nodeStore.put(nodeId, DAGNode.of(node));
    }

    public @Override void saveNodes(Map<NodeId, DAGNode> nodeMappings) {
        nodeStore.putAll(nodeMappings);
    }

    public @Override @Nullable RevTree getTree(ObjectId originalId) {
        return objectStore.getTree(originalId);
    }
}
