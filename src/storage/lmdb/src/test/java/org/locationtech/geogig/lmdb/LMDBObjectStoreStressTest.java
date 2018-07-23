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

import java.io.File;

import org.locationtech.geogig.repository.Platform;
import org.locationtech.geogig.storage.ConfigDatabase;
import org.locationtech.geogig.storage.ObjectStore;
import org.locationtech.geogig.test.performance.AbstractObjectStoreStressTest;

public class LMDBObjectStoreStressTest extends AbstractObjectStoreStressTest {

    protected @Override ObjectStore createDb(Platform platform, ConfigDatabase config) {
        File directory = super.tmp.getRoot();
        LMDBObjectStore store = new LMDBObjectStore(directory, "test-db");
        store.open();
        return store;
    }

}
