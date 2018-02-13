package com.upserve.uppend;

import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;

import java.io.*;
import java.lang.reflect.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.Paths;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;


public class BlobsTest {
    private Blobs blobs;

    @Before
    public void initialize() {
        blobs = new Blobs(Paths.get("build/test/blobs"));
        blobs.clear();
    }

    @After
    public void uninitialize() throws IOException {
        blobs.close();
        SafeDeleting.removeDirectory(Paths.get("build/test/blobs"));
    }

    @Test
    public void testSimple() {
        long pos = blobs.append("foo".getBytes());
        assertEquals(0, pos);
        pos = blobs.append("bar".getBytes());
        assertEquals(12, pos);
        byte[] bytes = blobs.read(0);
        assertEquals("foo", new String(bytes));
        bytes = blobs.read(12);
        assertEquals("bar", new String(bytes));
    }

    @Test
    public void testClear(){
        long pos = blobs.append("foo".getBytes());
        assertEquals(0, pos);
        pos = blobs.append("bar".getBytes());
        assertEquals(12, pos);
        blobs.clear();
        pos = blobs.append("baz".getBytes());
        assertEquals(0, pos);
    }

    @Test
    public void testClose(){
        assertEquals(0, blobs.append("foo".getBytes()));
        blobs.close();
        blobs.close();
        blobs = new Blobs(Paths.get("build/test/blobs"));
        assertEquals("foo", new String(blobs.read(0)));
    }

    @Test(expected = UncheckedIOException.class)
    public void testCloseException() throws Exception {
        Object previousBlobs = resetFinal(blobs, "blobs", new AsynchronousFileChannel() {
            @Override
            public boolean isOpen() {
                return false;
            }

            @Override
            public void close() throws IOException {
                throw new IOException("expected");
            }

            @Override
            public long size() throws IOException {
                return 0;
            }

            @Override
            public AsynchronousFileChannel truncate(long size) throws IOException {
                return null;
            }

            @Override
            public void force(boolean metaData) throws IOException {

            }

            @Override
            public <A> void lock(long position, long size, boolean shared, A attachment, CompletionHandler<FileLock, ? super A> handler) {

            }

            @Override
            public Future<FileLock> lock(long position, long size, boolean shared) {
                return null;
            }

            @Override
            public FileLock tryLock(long position, long size, boolean shared) throws IOException {
                return null;
            }

            @Override
            public <A> void read(ByteBuffer dst, long position, A attachment, CompletionHandler<Integer, ? super A> handler) {

            }

            @Override
            public Future<Integer> read(ByteBuffer dst, long position) {
                return null;
            }

            @Override
            public <A> void write(ByteBuffer src, long position, A attachment, CompletionHandler<Integer, ? super A> handler) {

            }

            @Override
            public Future<Integer> write(ByteBuffer src, long position) {
                return null;
            }
        });
        try {
            blobs.close();
        } finally {
            resetFinal(blobs, "blobs", previousBlobs);
        }
    }

    private static Object resetFinal(Object inst, String fieldName, Object val) throws Exception {
        Field field = inst.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        Object previous = field.get(inst);
        field.set(inst, val);
        return previous;
    }
}
