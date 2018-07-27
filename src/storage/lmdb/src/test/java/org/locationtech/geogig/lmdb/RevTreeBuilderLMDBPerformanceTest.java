/* Copyright (c) 2016 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.lmdb;

import java.io.IOException;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.locationtech.geogig.storage.ObjectStore;
import org.locationtech.geogig.test.performance.RevTreeBuilderPerformanceTest;

public class RevTreeBuilderLMDBPerformanceTest extends RevTreeBuilderPerformanceTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Override
    protected ObjectStore createObjectStore() throws IOException {
        LMDBObjectStore store = new LMDBObjectStore(tmp.getRoot(), "test-db");
        store.open();
        return store;
    }

}
