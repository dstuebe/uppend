package com.upserve.uppend;

import com.upserve.uppend.util.ThreadLocalByteBuffers;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Supplier;

public class Blobs implements AutoCloseable, Flushable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final int METADATA_LENGTH = 8 /* pos */ + 4 /* length */;

    private static final Supplier<ByteBuffer> METADATA_BUFFER_SUPPLIER = ThreadLocalByteBuffers.threadLocalByteBufferSupplier(METADATA_LENGTH);

    private static final Set<OpenOption> OPEN_OPTIONS = new LinkedHashSet<>(Arrays.asList(
            StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE
    ));

    private static final int BLOBS_EXEC_POOL_NUM_THREADS = 100;
    private static final ExecutorService BLOBS_EXEC_POOL;

    static {
        ThreadGroup threadGroup = new ThreadGroup("blobs");
        threadGroup.setDaemon(true);
        AtomicInteger threadNum = new AtomicInteger();
        ThreadFactory threadFactory = r -> new Thread(threadGroup, r, "blobs-exec-pool-" + threadNum.incrementAndGet());
        BLOBS_EXEC_POOL = Executors.newFixedThreadPool(BLOBS_EXEC_POOL_NUM_THREADS, threadFactory);
    }

    private final Path blobsFile;
    private final AsynchronousFileChannel blobs;
    private final AtomicLong blobsPosition;

    private final Path metadataFile;
    private final AsynchronousFileChannel metadata;
    private final AtomicLong metadataPosition;

    public Blobs(Path file) {
        this.blobsFile = file;
        this.metadataFile = file.resolveSibling(file.getFileName() + ".meta");

        Path dir = file.getParent();
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to mkdirs: " + dir, e);
        }

        try {
            blobs = AsynchronousFileChannel.open(file, OPEN_OPTIONS, BLOBS_EXEC_POOL);
            blobsPosition = new AtomicLong(blobs.size());
        } catch (IOException e) {
            throw new UncheckedIOException("unable to init blobs: " + blobsFile, e);
        }

        try {
            metadata = AsynchronousFileChannel.open(metadataFile, OPEN_OPTIONS, BLOBS_EXEC_POOL);
            metadataPosition = new AtomicLong(metadata.size());

            if (metadataPosition.get() % METADATA_LENGTH != 0) {
                long expectedMetadataLength = metadataPosition.get() / METADATA_LENGTH * METADATA_LENGTH;
                log.warn("trimming {} from {} to {}", metadataFile, metadataPosition.get(), expectedMetadataLength);
                metadata.truncate(expectedMetadataLength);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("unable to init metadata: " + metadataFile, e);
        }
    }

    public long append(byte[] bytes) {
        long blobsPos = blobsPosition.getAndAdd(bytes.length);
        write(blobs, ByteBuffer.wrap(bytes), blobsPos, bytes.length, blobsFile);

        long metadataPos = metadataPosition.getAndAdd(METADATA_LENGTH);
        ByteBuffer metadataBuf = METADATA_BUFFER_SUPPLIER.get();
        metadataBuf.putLong(blobsPos);
        metadataBuf.putInt(bytes.length);
        metadataBuf.flip();
        write(metadata, metadataBuf, metadataPos, METADATA_LENGTH, metadataFile);

        log.trace("appended {} bytes to blob file {} at pos {}; appended {} bytes to metadata file {} at pos {}", bytes.length, blobsFile, blobsPos, METADATA_LENGTH, metadataFile, metadataPos);
        return metadataPos;
    }

    public byte[] read(long pos) {
        log.trace("reading metadata from {} @ {}", metadataFile, pos);
        ByteBuffer metadataBuf = METADATA_BUFFER_SUPPLIER.get();
        read(metadata, metadataBuf, pos, METADATA_LENGTH, metadataFile);
        metadataBuf.flip();
        long blobsPos = metadataBuf.getLong();
        int size = metadataBuf.getInt();
        byte[] buf = new byte[size];
        read(blobs, ByteBuffer.wrap(buf), blobsPos, size, blobsFile);
        log.trace("read blob of {} bytes from {} @ {}", size, blobsFile, blobsPos);
        return buf;
    }

    public void clear() {
        log.trace("clearing metadata {}", metadataFile);
        try {
            metadata.truncate(0);
            metadataPosition.set(0);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to clear metadata: " + metadataFile, e);
        }
        log.trace("clearing blobs {}", blobsFile);
        try {
            blobs.truncate(0);
            blobsPosition.set(0);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to clear blobs: " + blobsFile, e);
        }
    }

    @Override
    public void close() {
        log.trace("closing blobs {}", blobsFile);
        try {
            blobs.close();
        } catch (IOException e) {
            throw new UncheckedIOException("unable to close blobs " + blobsFile, e);
        }
        log.trace("closing metadata {}", metadataFile);
        try {
            metadata.close();
        } catch (IOException e) {
            throw new UncheckedIOException("unable to close metadata " + metadataFile, e);
        }
    }

    @Override
    public void flush() {
        try {
            blobs.force(true);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to flush blobs: " + blobsFile, e);
        }
        try {
            metadata.force(true);
        } catch (IOException e) {
            throw new UncheckedIOException("unable to flush metadata: " + metadataFile, e);
        }
    }

    public static byte[] read(AsynchronousFileChannel blobsChan, AsynchronousFileChannel metadataChan, long pos) {
        log.trace("reading metadata @ {}", pos);
        ByteBuffer metadataBuf = METADATA_BUFFER_SUPPLIER.get();
        read(metadataChan, metadataBuf, pos, METADATA_LENGTH, "unknown(metadata)");
        metadataBuf.flip();
        long blobsPos = metadataBuf.getLong();
        int size = metadataBuf.getInt();
        byte[] buf = new byte[size];
        read(blobsChan, ByteBuffer.wrap(buf), blobsPos, size, "unknown(blobs)");
        log.trace("read blob of {} bytes @ {}", size, blobsPos);
        return buf;
    }

    private static void write(AsynchronousFileChannel chan, ByteBuffer buf, long pos, int length, Object file) {
        try {
            int numWritten = chan.write(buf, pos).get();
            if (numWritten != length) {
                throw new RuntimeException("bad write to " + file + " at pos " + pos + ": expected to write " + length + " bytes, wrote " + numWritten);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("interrupted while writing " + length + " bytes to " + file + " at pos " + pos);
        } catch (ExecutionException e) {
            throw new RuntimeException("trouble while writing " + length + " bytes to " + file + " at pos " + pos, e);
        }
    }

    private static void read(AsynchronousFileChannel chan, ByteBuffer buf, long pos, int length, Object file) {
        try {
            int numRead = chan.read(buf, pos).get();
            if (numRead != length) {
                throw new RuntimeException("bad read from " + file + " at pos " + pos + ": expected to read " + length + " bytes, read " + numRead);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("interrupted while reading " + length + " bytes from " + file + " at pos " + pos);
        } catch (ExecutionException e) {
            throw new RuntimeException("trouble while reading " + length + " bytes from " + file + " at pos " + pos, e);
        }
    }

}
