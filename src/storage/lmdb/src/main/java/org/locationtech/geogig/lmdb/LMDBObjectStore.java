package org.locationtech.geogig.lmdb;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterators.spliteratorUnknownSize;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.lmdbjava.Cursor;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.GetOp;
import org.lmdbjava.PutFlags;
import org.lmdbjava.SeekOp;
import org.lmdbjava.Txn;
import org.locationtech.geogig.model.NodeRef;
import org.locationtech.geogig.model.ObjectId;
import org.locationtech.geogig.model.RevObject;
import org.locationtech.geogig.model.RevObject.TYPE;
import org.locationtech.geogig.model.internal.ByteBufferInputStream;
import org.locationtech.geogig.storage.AutoCloseableIterator;
import org.locationtech.geogig.storage.BulkOpListener;
import org.locationtech.geogig.storage.ObjectInfo;
import org.locationtech.geogig.storage.ObjectStore;
import org.locationtech.geogig.storage.datastream.v2_3.DataStreamSerializationFactoryV2_3;
import org.locationtech.geogig.storage.impl.AbstractObjectStore;

import com.google.common.base.Stopwatch;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LMDBObjectStore extends AbstractObjectStore implements ObjectStore {

    private final AtomicBoolean open = new AtomicBoolean();

    private final File directory;

    private final String dbName;

    private Env<ByteBuffer> env;

    private Dbi<ByteBuffer> db;

    public LMDBObjectStore(final @NonNull File directory, final @NonNull String dbName) {
//        super(DataStreamSerializationFactoryV2_3.INSTANCE);
        this.directory = directory;
        this.dbName = dbName;

    }

    public synchronized @Override void open() {
        if (!isOpen()) {
            env = Env.create()//
                    .setMapSize(100L * 1024 * 1024 * 1024)//
                    .setMaxReaders(128)//
                    .setMaxDbs(1)//
                    .open(directory, EnvFlags.MDB_WRITEMAP, EnvFlags.MDB_MAPASYNC);// ,
                                                                                   // EnvFlags.MDB_NOMETASYNC,
                                                                                   // EnvFlags.MDB_NOSYNC);

            db = env.openDbi(dbName, DbiFlags.MDB_CREATE);
            open.set(true);
        }
    }

    public @Override void close() {
        if (this.open.getAndSet(false)) {
            db.close();
            env.close();
        }
    }

    public @Override boolean isOpen() {
        return open.get();
    }

    public @Override boolean exists(final @NonNull ObjectId id) {
        checkOpen();
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            final ByteBuffer found = db.get(txn, key(id));
            return found != null;
        }
    }

    public @Override void delete(final @NonNull ObjectId objectId) {
        checkOpen();
        db.delete(key(objectId));
    }

    protected @Override boolean putInternal(@NonNull ObjectId id, final @NonNull byte[] rawData) {
        checkOpen();
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            boolean put = db.put(txn, key(id), data(rawData), PutFlags.MDB_NOOVERWRITE);
            txn.commit();
            return put;
        }
    }

    public @Override Iterator<RevObject> getAll(Iterable<ObjectId> ids, BulkOpListener listener) {
        return getAll(ids, listener, RevObject.class);
    }

    public @Override <T extends RevObject> Iterator<T> getAll(@NonNull Iterable<ObjectId> ids,
            @NonNull BulkOpListener listener, @NonNull Class<T> type) {
        checkOpen();

        return new GetAllIterator<>(this, ids.iterator(), listener, type);
    }

    private static @RequiredArgsConstructor class GetAllIterator<R extends RevObject>
            extends AbstractIterator<R> {

        private final LMDBObjectStore store;

        private final Iterator<ObjectId> ids;

        private final BulkOpListener listener;

        private final Class<R> type;

        ByteBuffer key = ByteBuffer.allocateDirect(ObjectId.NUM_BYTES);

        byte[] keybuff = new byte[ObjectId.NUM_BYTES];

        byte[] data = new byte[4096];

        Txn<ByteBuffer> txn;

        protected @Override R computeNext() {
            if (!ids.hasNext()) {
                if (txn != null) {
                    txn.close();
                }
                return endOfData();
            }
            if (txn == null) {
                txn = store.env.txnRead();
            }
            ObjectId id = ids.next();
            id.getRawValue(keybuff);
            key.put(keybuff).flip();
            ByteBuffer val = store.db.get(txn, key);
            if (val == null) {
                listener.notFound(id);
                return computeNext();
            }

            int length = val.remaining();
            data = ensureCapacity(data, length);
            val.get(data, 0, length);
            try {
                RevObject obj = store.serializer().read(id, data, 0, length);
                if (type.isInstance(obj)) {
                    listener.found(id, null);
                    return type.cast(obj);
                } else {
                    listener.notFound(id);
                    return computeNext();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private byte[] ensureCapacity(byte[] data, int size) {
            if (data.length < size) {
                data = new byte[2 * size];
            }
            return data;
        }

    }

    public @Override void deleteAll(final @NonNull Iterator<ObjectId> ids,
            final @NonNull BulkOpListener listener) {

        checkOpen();
        ByteBuffer key = ByteBuffer.allocateDirect(ObjectId.NUM_BYTES);
        byte[] buff = new byte[ObjectId.NUM_BYTES];

        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            ids.forEachRemaining(id -> {
                id.getRawValue(buff);
                key.put(buff).flip();
                boolean deleted = db.delete(txn, key);
                if (deleted) {
                    listener.deleted(id);
                } else {
                    listener.notFound(id);
                }
            });
            txn.commit();
        }
    }

    public @Override void putAll(@NonNull Iterator<? extends RevObject> objects,
            final @NonNull BulkOpListener listener) {

        checkOpen();
        Stream<RevObject> stream = toStream(objects, listener);
        putAll(stream, listener);
    }

    protected void putAll(Stream<RevObject> stream, BulkOpListener listener) {
        final Stopwatch sw = log.isTraceEnabled() ? Stopwatch.createStarted() : null;

        // encodes on several threads
        Stream<EncodedObject> encoded = stream.parallel().map(o -> encode(o));

        final int batchsize = 100;

        int insertCount = 0;
        final Iterator<EncodedObject> iterator = encoded.iterator();
        while (iterator.hasNext()) {
            Iterator<EncodedObject> batch = Iterators.limit(iterator, batchsize);
            insertCount += insertBatch(batch, listener);
        }
        if (log.isTraceEnabled()) {
            log.trace(String.format("Inserted %,d objects in %s", insertCount, sw.stop()));
        }
    }

    private int insertBatch(Iterator<EncodedObject> batch, BulkOpListener listener) {
        ByteBuffer key = ByteBuffer.allocateDirect(ObjectId.NUM_BYTES);
        ByteBuffer val = ByteBuffer.allocateDirect(4096);

        byte[] keybuff = new byte[ObjectId.NUM_BYTES];

        List<ObjectId> inserted = new ArrayList<>();

        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            while (batch.hasNext()) {
                EncodedObject o = batch.next();
                o.id.getRawValue(keybuff);
                key.put(keybuff).flip();
                InternalBAOS out = o.serialform;
                val = ensureCapacity(val, out.size());
                val.position(0).limit(val.capacity());
                val.put(out.intenal(), 0, out.size()).flip();

                boolean put = db.put(txn, key, val, PutFlags.MDB_NOOVERWRITE);
                if (put) {
                    inserted.add(o.id);
                } else {
                    listener.found(o.id, null);
                }
            }

            txn.commit();
            inserted.forEach(id -> listener.inserted(id, null));
        }
        return inserted.size();
    }

    protected Stream<RevObject> toStream(Iterator<? extends RevObject> objects,
            BulkOpListener listener) {

        final int characteristics = IMMUTABLE | NONNULL | DISTINCT;
        Stream<RevObject> stream;
        stream = StreamSupport.stream(spliteratorUnknownSize(objects, characteristics), true);
        return stream;
    }

    private EncodedObject encode(RevObject o) {
        InternalBAOS out = new InternalBAOS();
        try {
            serializer().write(o, out);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new EncodedObject(o.getId(), o.getType(), out);
    }

    protected @RequiredArgsConstructor static class EncodedObject {
        final ObjectId id;

        final TYPE type;

        final InternalBAOS serialform;
    }

    private ByteBuffer ensureCapacity(ByteBuffer val, int size) {
        if (val.capacity() < size) {
            int newCapacity = 2 * size;
            val = ByteBuffer.allocateDirect(newCapacity);
        }
        return val;
    }

    public @Override <T extends RevObject> AutoCloseableIterator<ObjectInfo<T>> getObjects(
            final @NonNull Iterator<NodeRef> nodes, final @NonNull BulkOpListener listener,
            final @NonNull Class<T> type) {
        return null;
    }

    protected @Override List<ObjectId> lookUpInternal(final @NonNull byte[] raw) {
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            ByteBuffer key = key(raw);
            try (Cursor<ByteBuffer> c = db.openCursor(txn)) {
                boolean found = c.get(key, GetOp.MDB_SET_RANGE);
                List<ObjectId> matches = new ArrayList<>();
                while (found) {
                    ObjectId id = toObjectId(c.key());
                    byte[] target = new byte[raw.length];
                    id.getRawValue(target, target.length);
                    found = Arrays.equals(raw, target);
                    if (found) {
                        matches.add(id);
                        found = c.seek(SeekOp.MDB_NEXT);
                    }
                }
                return matches;
            }
        }
    }

    private ObjectId toObjectId(ByteBuffer key) {
        return ObjectId.create(key.getInt(), key.getLong(), key.getLong());
    }

    protected @Override InputStream getRawInternal(@NonNull ObjectId id,
            final boolean failIfNotFound) throws IllegalArgumentException {

        checkOpen();
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            final ByteBuffer found = db.get(txn, key(id));
            if (found == null) {
                if (failIfNotFound) {
                    throw new IllegalArgumentException("Object " + id + " does not exist");
                }
                return null;
            }
            return new ByteBufferInputStream(found);
        }
    }

    private ByteBuffer key(final ObjectId id) {
        byte[] raw = id.getRawValue();
        return key(raw);
    }

    private ByteBuffer key(byte[] raw) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(raw.length);
        buffer.put(raw).flip();
        return buffer;
    }

    private ByteBuffer data(byte[] data) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(data.length);
        buffer.put(data).flip();
        return buffer;
    }

    private void checkOpen() {
        checkState(isOpen(), "db is closed");
    }

    private static final class InternalBAOS extends ByteArrayOutputStream {
        InternalBAOS() {
            super(4096);
        }

        public byte[] intenal() {
            return super.buf;
        }
    }
}
