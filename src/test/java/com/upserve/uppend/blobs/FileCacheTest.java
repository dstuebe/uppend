package com.upserve.uppend.blobs;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.file.*;
import java.util.Random;
import java.util.concurrent.CompletionException;

import static org.hamcrest.core.IsInstanceOf.any;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileCacheTest {

    Path rootPath = Paths.get("build/test/blobs/file_cache");
    Path existingFile = rootPath.resolve("existing_file");
    Path fileDoesNotExist = rootPath.resolve("file_does_not_exist");
    Path pathDoesNotExist = rootPath.resolve("path_does_not_exist/file");

    FileCache instance;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void before() throws IOException {
        SafeDeleting.removeDirectory(rootPath);
        Files.createDirectories(rootPath);
        Files.createFile(existingFile);
    }

    @After
    public void after() throws InterruptedException {
        if (instance != null) {
            instance.flush();
            Thread.sleep(100); // TODO fix executors for cache so we don't need this
        }
    }

    @Test
    public void testReadOnlyNonExistentFile(){
        instance = new FileCache(64, 256, true);
        assertTrue(instance.readOnly());

        assertEquals(null, instance.getEntryIfPresent(fileDoesNotExist));

        thrown.expect(CompletionException.class);
        thrown.expectCause(any(NoSuchFileException.class));

        instance.getEntry(fileDoesNotExist);
    }

    @Test
    public void testReadOnlyExists() throws InterruptedException {
        testHelper(existingFile, true);
    }

    @Test
    public void testReadWriteFileDoesExists() throws InterruptedException {
        testHelper(existingFile, false);
    }

    @Test
    public void testReadWriteFileDoesNotExist() throws InterruptedException {
        testHelper(fileDoesNotExist, false);
    }

    public void testHelper(Path path, boolean readOnly) throws InterruptedException {
        instance = new FileCache(64, 256, readOnly);
        assertEquals(readOnly, instance.readOnly());

        assertEquals(null, instance.getEntryIfPresent(path));

        FileCacheEntry newFileChannel = instance.getEntry(path);
        assertEquals(newFileChannel, instance.getEntryIfPresent(path));
        assertTrue(newFileChannel.fileChannel.isOpen());

        instance.flush();

        // Wait for async removal listener
        Thread.sleep(100);

        assertFalse(newFileChannel.fileChannel.isOpen());

        assertEquals(null, instance.getEntryIfPresent(path));
    }

    @Test
    public void testReadWritePathDoesNotExist() {
        instance = new FileCache(64, 256, false);
        assertFalse(instance.readOnly());

        thrown.expect(CompletionException.class);
        thrown.expectCause(any(NoSuchFileException.class));

        FileCacheEntry fc = instance.getEntry(pathDoesNotExist);
        assertEquals(null, fc);
    }

    @Test
    public void testHammerFileCache(){
        instance = new FileCache(64, 256, false);

        final int requests = 1024 * 1024;

        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putInt(10);
        byteBuffer.flip();

        new Random()
                .ints(requests, 0, 1024)
                .parallel()
                .forEach(val -> {
                    int retries = 5;
                    try {
                        while (retries > 0) {
                            try {
                               instance.getEntry(rootPath.resolve("tst" + val)).fileChannel.write(byteBuffer);
                               break;
                            } catch (ClosedChannelException e) {
                                retries--;
                            }
                        }
                    } catch (IOException e) {
                        throw new UncheckedIOException("Could not write to tst" + val, e);
                    }
                });
        CacheStats stats = instance.stats();
        assertEquals(requests, stats.requestCount());
        assertEquals(0.25, stats.hitRate(), 0.05);
    }
}
