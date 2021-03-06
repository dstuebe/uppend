package com.upserve.uppend.lookup;

import com.google.common.primitives.Ints;
import com.upserve.uppend.*;
import com.upserve.uppend.blobs.*;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.*;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

@RunWith(MockitoJUnitRunner.class)
public class LookupDataTest {
    private static final int RELOAD_INTERVAL = -1;
    private static final int FLUSH_THRESHOLD = -1;

    private final String name = "lookupdata-test";
    private final Path lookupDir = Paths.get("build/test/lookup").resolve(name);
    private AppendOnlyStoreBuilder defaults = TestHelper
            .getDefaultAppendStoreTestBuilder();

    private VirtualPageFile metadataPageFile;
    private VirtualMutableBlobStore mutableBlobStore;

    private VirtualPageFile keyDataPageFile;
    private VirtualLongBlobStore keyBlobStore;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    public static final int NUMBER_OF_STORES = 12;

    @Before
    public void initialize() throws Exception {
        SafeDeleting.removeDirectory(lookupDir);
        Files.createDirectories(lookupDir);
        setup(false);
    }

    public void setup(boolean readOnly) {
        metadataPageFile = new VirtualPageFile(lookupDir.resolve("metadata"), NUMBER_OF_STORES, 1024, 16384, readOnly);
        mutableBlobStore = new VirtualMutableBlobStore(1, metadataPageFile);

        keyDataPageFile = new VirtualPageFile(lookupDir.resolve("keydata"), NUMBER_OF_STORES, defaults.getLookupPageSize(), defaults.getTargetBufferSize(), readOnly);
        keyBlobStore = new VirtualLongBlobStore(1, keyDataPageFile);
    }

    @After
    public void tearDown() throws IOException {
        keyDataPageFile.close();
        metadataPageFile.close();
    }

    @Test
    public void testOpenEmptyReadOnly() throws IOException {
        tearDown(); // Close the page files
        setup(true);
        LookupData data = LookupData.lookupReader(keyBlobStore, mutableBlobStore, RELOAD_INTERVAL);
        final LookupKey key = new LookupKey("mykey");
        assertNull(data.getValue(key));

        thrown.expect(RuntimeException.class);
        data.put(key, 1L);
    }

    @Test
    public void testOpenGetAndPut() {
        LookupData data = LookupData.lookupWriter(keyBlobStore, mutableBlobStore, FLUSH_THRESHOLD);
        final LookupKey key = new LookupKey("mykey");
        assertNull(data.getValue(key));
        data.put(key, 80);
        assertEquals(Long.valueOf(80), data.getValue(key));
    }

    @Test
    public void testPutIfNotExists() {
        LookupData data = LookupData.lookupWriter(keyBlobStore, mutableBlobStore, FLUSH_THRESHOLD);
        final LookupKey key = new LookupKey("mykey");
        assertNull("The key should not exist yet", data.getValue(key));
        data.putIfNotExists(key, 1);
        assertEquals(Long.valueOf(1), data.getValue(key));
        data.putIfNotExists(key, 2);
        assertEquals(Long.valueOf(1), data.getValue(key));
    }

    @Test
    public void testPutIfNotExistsFunction() {
        LookupData data = LookupData.lookupWriter(keyBlobStore, mutableBlobStore, FLUSH_THRESHOLD);
        final LookupKey key = new LookupKey("mykey");
        assertNull("The key should not exist yet", data.getValue(key));
        data.putIfNotExists(key, () -> 1);
        assertEquals(Long.valueOf(1), data.getValue(key));
        data.putIfNotExists(key, () -> 2);
        assertEquals(Long.valueOf(1), data.getValue(key));
    }

    @Test
    public void testPut() {
        LookupData data = LookupData.lookupWriter(keyBlobStore, mutableBlobStore, FLUSH_THRESHOLD);
        final LookupKey key = new LookupKey("mykey");
        assertNull("The key should not exist yet", data.getValue(key));
        data.put(key, 1);
        assertEquals(Long.valueOf(1), data.getValue(key));
        data.put(key, 2);
        assertEquals(Long.valueOf(2), data.getValue(key));
    }

    @Test
    public void testIncrement() {
        LookupData data = LookupData.lookupWriter(keyBlobStore, mutableBlobStore, FLUSH_THRESHOLD);
        final LookupKey key = new LookupKey("mykey");
        assertNull("The key should not exist yet", data.getValue(key));
        data.increment(key, 10);
        assertEquals(Long.valueOf(10), data.getValue(key));
        data.increment(key, 2);
        assertEquals(Long.valueOf(12), data.getValue(key));
    }

    @Test
    public void testFlushAndClose() throws IOException {
        LookupData data = LookupData.lookupWriter(keyBlobStore, mutableBlobStore, FLUSH_THRESHOLD);
        final LookupKey key = new LookupKey("mykey");
        data.put(key, 80);
        data.flush();

        Long result = data.getValue(key);
        assertEquals(Long.valueOf(80), result);

        tearDown();
        setup(true);

        data = LookupData.lookupReader(keyBlobStore, mutableBlobStore, RELOAD_INTERVAL);
        result = data.getValue(key);
        assertEquals(Long.valueOf(80), result);
    }

    @Test
    public void testWriteCacheUnderLoad() throws IOException {
        LookupData data = LookupData.lookupWriter(keyBlobStore, mutableBlobStore, FLUSH_THRESHOLD);

        LongStream.range(0, 100_000)
                .forEach(val -> {
                    data.putIfNotExists(new LookupKey(String.valueOf(val)), val);
                });

        assertEquals(100_000, data.writeCache.size());

        data.flush();

        assertEquals(0, data.writeCache.size());

        LongStream.range(0, 100_000)
                .forEach(val -> {
                    data.putIfNotExists(new LookupKey(String.valueOf(val)), val);
                });

        LongStream.range(0, 100_010)
                .forEach(val -> {
                    data.putIfNotExists(new LookupKey(String.valueOf(val)), val);
                });
        assertEquals(10, data.writeCache.size());
    }

    @Test
    public void testScan() throws IOException {
        LookupData data = LookupData.lookupWriter(keyBlobStore, mutableBlobStore, FLUSH_THRESHOLD);
        LookupKey firstKey = new LookupKey("mykey1");
        LookupKey secondKey = new LookupKey("mykey2");

        data.put(firstKey, 1);
        data.put(secondKey, 2);

        assertEquals(0, data.flushCache.size());
        assertEquals(2, data.writeCache.size());

        scanTestHelper(data, new LookupKey[]{firstKey, secondKey}, new Long[]{1L, 2L});

        data.flushWriteCache(data.getMetadata());

        assertEquals(2, data.flushCache.size());
        assertEquals(0, data.writeCache.size());

        scanTestHelper(data, new LookupKey[]{firstKey, secondKey}, new Long[]{1L, 2L});

        data.generateMetaData(data.getMetadata());

        assertEquals(2, data.flushCache.size());
        assertEquals(0, data.writeCache.size());

        scanTestHelper(data, new LookupKey[]{firstKey, secondKey}, new Long[]{1L, 2L});

        data.flushCache.clear();

        assertEquals(0, data.flushCache.size());
        assertEquals(0, data.writeCache.size());

        scanTestHelper(data, new LookupKey[]{firstKey, secondKey}, new Long[]{1L, 2L});

        tearDown();
        setup(true);

        data = LookupData.lookupWriter(keyBlobStore, mutableBlobStore, FLUSH_THRESHOLD);
        scanTestHelper(data, new LookupKey[]{firstKey, secondKey}, new Long[]{1L, 2L});
    }

    private void scanTestHelper(LookupData data, LookupKey[] expectedKeys, Long[] expectedValues) {
        ConcurrentMap<LookupKey, Long> entries = new ConcurrentHashMap<>();
        data.scan(entries::put);
        assertEquals(2, entries.size());
        assertArrayEquals(expectedKeys, entries.keySet().stream().sorted().toArray(LookupKey[]::new));
        assertArrayEquals(expectedValues, entries.values().stream().sorted().toArray(Long[]::new));

        entries = data.scan().collect(Collectors.toConcurrentMap(Map.Entry::getKey, Map.Entry::getValue));
        assertEquals(2, entries.size());
        assertArrayEquals(expectedKeys, entries.keySet().stream().sorted().toArray(LookupKey[]::new));
        assertArrayEquals(expectedValues, entries.values().stream().sorted().toArray(Long[]::new));

        entries = data.keys().collect(Collectors.toConcurrentMap(Function.identity(), v -> 1L));
        assertEquals(2, entries.size());
        assertArrayEquals(expectedKeys, entries.keySet().stream().sorted().toArray(LookupKey[]::new));
    }

    @Test
    public void testScanNonExistant() {
        LookupData data = LookupData.lookupWriter(keyBlobStore, mutableBlobStore, FLUSH_THRESHOLD);
        data.scan((k, v) -> {
            throw new IllegalStateException("should not have called this");
        });

        data.scan().forEach(entry -> {
            throw new IllegalStateException("should not have called this");
        });

        data.keys().forEach(key -> {
            throw new IllegalStateException("should not have called this");
        });
    }

    @Test
    public void testLoadReadOnlyMetadata() {

        mutableBlobStore.write(0, Ints.toByteArray(50));
        mutableBlobStore.write(4, Ints.toByteArray(284482732)); // Check checksum
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Checksum did not match for the requested blob");

        LookupData data = LookupData.lookupReader(keyBlobStore, mutableBlobStore, RELOAD_INTERVAL);
    }

    @Test
    public void testLoadReadRepairMetadata() {
        LookupData data = LookupData.lookupWriter(keyBlobStore, mutableBlobStore, FLUSH_THRESHOLD);

        Random random = new Random();
        LongStream.range(0, 100_000)
                .parallel()
                .forEach(val -> {
                    byte[] bytes = new byte[(int) (val % 64)];
                    random.nextBytes(bytes);
                    data.put(new LookupKey(bytes), val);
                });

        data.flush();

        LookupMetadata expected = data.getMetadata();

        mutableBlobStore.write(0, Ints.toByteArray(50));
        mutableBlobStore.write(4, Ints.toByteArray(284482732)); // Invalid Check checksum

        // Do read repair!
        LookupMetadata result = data.loadMetadata();

        // It is a new object!
        assertNotEquals(expected, result);

        assertEquals(expected.getMinKey(), result.getMinKey());
        assertEquals(expected.getMaxKey(), result.getMaxKey());

        assertArrayEquals(expected.getKeyStorageOrder(), result.getKeyStorageOrder());
    }

    @Test
    public void testFlushWithAppendLoad() throws ExecutionException, InterruptedException {
        LookupData data = LookupData.lookupWriter(keyBlobStore, mutableBlobStore, 100);

        int n = 500;

        Thread flusher = new Thread(() -> {
            for (int i = 0; i < n; i++) {
                data.flush();
            }
        });

        Random random = new Random();
        Thread writer = new Thread(() -> {
            for (int j = 0; j < n; j++) {
                random.longs(128, 0 , 1000)
                        .parallel()
                        .forEach(val -> {
                            byte[] bytes = new byte[(int) (val % 64)];
                            random.nextBytes(bytes);
                            data.put(new LookupKey(bytes), val);
                        });
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Interrupted", e);
                }
            }
        });

        writer.start();
        flusher.start();

        writer.join();
    }

    @Test
    public void testGetMetadataReloadDeactivated() {
        LookupData data = Mockito.spy(LookupData.lookupReader(keyBlobStore, mutableBlobStore, RELOAD_INTERVAL));
        int[] stamp = new int[1];
        LookupMetadata expected = data.timeStampedMetadata.get(stamp);

        // Even when expired - don't reload
        data.timeStampedMetadata.set(expected, -5);
        data.reloadStamp.set(-5);
        LookupMetadata lmd1 = data.getMetadata();
        assertSame(
                "When the reload interval less than zero it should never reload",
                expected,
                lmd1
        );
        Mockito.verify(data, never()).loadMetadata();
    }

    @Test
    public void testGetMetadataShouldReload() {
        LookupData data = Mockito.spy(LookupData.lookupReader(keyBlobStore, mutableBlobStore, 50));
        int[] stamp = new int[1];
        LookupMetadata expected = data.timeStampedMetadata.get(stamp);

        // Set timestamp and reload concurrent-access value for compare and set operation
        data.timeStampedMetadata.set(expected, -5);
        data.reloadStamp.set(-5);

        LookupMetadata lmd1 = data.getMetadata();
        assertNotSame(
                "When the reload is active (GT 0) and the time appears to be expired it should reload",
                expected,
                lmd1
        );

        LookupMetadata lmd2 = data.getMetadata();
        assertSame(
                "Don't reload again till the timestamp expires again",
                lmd1,
                lmd2
        );
        Mockito.verify(data, times(1)).loadMetadata(any());
    }

    @Test
    public void testGetMetadataShouldNotReload() {
        LookupData data = Mockito.spy(LookupData.lookupReader(keyBlobStore, mutableBlobStore, 50));
        int[] stamp = new int[1];
        LookupMetadata expected = data.timeStampedMetadata.get(stamp);

        LookupMetadata lmd0 = data.getMetadata();
        assertSame(
                "Timestamp not expired, so LookupMetadata instances should be identical",
                expected,
                lmd0
        );

        // Set the timestamp to be expired but leave the concurrent-access value so the compare and set fails
        data.timeStampedMetadata.set(expected, -5);

        // The lookup metadata is not reloaded
        LookupMetadata lmd1 = data.getMetadata();
        assertSame(
                "Timestamp expired but concurrent-access value not equal to timestamp",
                expected,
                lmd1
        );
        Mockito.verify(data, never()).loadMetadata();

        data.reloadStamp.set(-5);
        LookupMetadata lmd2 = data.getMetadata();

        assertNotSame(
                "Timestamp expired and concurrent-access value equal to timestamp; LookupMetadata reloaded",
                lmd2,
                expected
        );
        Mockito.verify(data, times(1)).loadMetadata(any());
    }

    @Test
    public void testGetMetadataIntegration() {
        // Make a reader with no keys
        LookupData dataReader = Mockito.spy(LookupData.lookupReader(keyBlobStore, mutableBlobStore, 50));
        int[] stamp = new int[1];
        LookupMetadata lmd0 = dataReader.timeStampedMetadata.get(stamp);
        assertEquals("There are no keys yet!", 0, lmd0.getNumKeys());

        // Make a writer and add a key
        LookupData dataWriter = LookupData.lookupWriter(keyBlobStore, mutableBlobStore, FLUSH_THRESHOLD);
        final LookupKey key1 = new LookupKey("mykey1");
        dataWriter.put(key1, 80);
        dataWriter.flush();

        // Expire the reader metadata
        dataReader.reloadStamp.set(-10);
        dataReader.timeStampedMetadata.set(lmd0, -10);
        LookupMetadata lmd1 = dataReader.getMetadata();
        assertNotSame(
                "with data in the mutableBlobStore, after expiration we should reload",
                lmd0,
                lmd1
        );
        assertEquals("we flushed one key and reloaded",lmd1.getNumKeys(), 1);
        Mockito.verify(dataReader, times(1)).loadMetadata(any());

        // add a second key & value to the blob store
        final LookupKey key2 = new LookupKey("mykey2");
        dataWriter.put(key2, 80);
        dataWriter.flush();

        LookupMetadata lmd2 = dataReader.getMetadata();
        assertSame(
                "Metadata is not expired so no reload, instances are the same",
                lmd2,
                lmd1
        );

        // Expire the timestamp but don't adjust the concurrent-access value
        dataReader.timeStampedMetadata.set(lmd1, -10);
        LookupMetadata lmd3 = dataReader.getMetadata();
        assertSame(
                "If the stamp has expired only the thread where the concurrent_access value matches will do the reload",
                lmd3,
                lmd1
        );

        // Adjust the concurrent-access value - now it will actually reload and return the new metadata
        dataReader.reloadStamp.set(-10);
        LookupMetadata lmd4 = dataReader.getMetadata();

        assertNotSame(
                "This call sees compare and set True then do the reload",
                lmd4,
                lmd1
        );
        assertEquals(lmd4.getNumKeys(), 2);
        Mockito.verify(dataReader, times(2)).loadMetadata(any());

        // Without any new keys, expire the timestamp and call reload but the returned metadata instance stays the same
        dataReader.reloadStamp.set(-10);
        dataReader.timeStampedMetadata.set(lmd4, -10);
        LookupMetadata lmd5 = dataReader.getMetadata();
        assertSame(
                "Without a new flush, the load operation should return the same metadata",
                lmd5,
                lmd4
        );
        Mockito.verify(dataReader, times(3)).loadMetadata(any());
    }
}
