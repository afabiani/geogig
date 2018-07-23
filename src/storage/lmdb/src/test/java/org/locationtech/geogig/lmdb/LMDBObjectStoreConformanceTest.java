/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.lmdb;

import java.io.File;
import java.io.IOException;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.locationtech.geogig.storage.ObjectStore;
import org.locationtech.geogig.storage.impl.ObjectStoreConformanceTest;

public class LMDBObjectStoreConformanceTest extends ObjectStoreConformanceTest {

    public @Rule TemporaryFolder folder = new TemporaryFolder();

    protected @Override ObjectStore createOpen() throws IOException {
        File directory = folder.getRoot();
        LMDBObjectStore store = new LMDBObjectStore(directory, "test-db");
        store.open();
        return store;
    }
}
