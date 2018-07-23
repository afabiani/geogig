/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.model.internal;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Adapts a {@link ByteBuffer} as a {@link InputStream}
 *
 */
public @RequiredArgsConstructor class ByteBufferInputStream extends InputStream {

    private final @NonNull ByteBuffer buf;

    public @Override int read() throws IOException {
        int c;
        if (buf.hasRemaining()) {
            c = buf.get() & 0xFF;
        } else {
            c = -1;
        }
        return c;
    }

    public @Override int read(byte[] bytes, int off, int len) throws IOException {
        if (!buf.hasRemaining()) {
            return -1;
        }
        len = Math.min(len, buf.remaining());
        buf.get(bytes, off, len);
        return len;
    }
}