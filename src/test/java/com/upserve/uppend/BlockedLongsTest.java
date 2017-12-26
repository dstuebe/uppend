package com.upserve.uppend;

import com.upserve.uppend.util.SafeDeleting;
import org.junit.*;

import java.io.UncheckedIOException;
import java.nio.file.*;

import static org.junit.Assert.*;

public class BlockedLongsTest {
    private Path path = Paths.get("build/test/tmp/block");
    private Path posPath = path.resolveSibling(path.getFileName() + ".pos");

    @Before
    public void initialize() throws Exception {
        SafeDeleting.removeTempPath(path);
        SafeDeleting.removeTempPath(posPath);
    }

    @Test
    public void testCtor() {
        new BlockedLongs(path, 1);
        new BlockedLongs(path, 10);
        new BlockedLongs(path, 100);
        new BlockedLongs(path, 1000);
    }

    @Test(expected = UncheckedIOException.class)
    public void testCtorNoPosFile() throws Exception {
        BlockedLongs block = new BlockedLongs(path, 1);
        block.close();
        Files.delete(posPath);
        Files.createDirectories(posPath);
        new BlockedLongs(path, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCtorWithNullFile() {
        new BlockedLongs(null, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCtorWithZeroValuesPerBlock() {
        new BlockedLongs(path, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCtorWithNegativeValuesPerBlock() {
        new BlockedLongs(path, -1);
    }

    @Test
    public void testAllocate() throws Exception {
        for (int i = 1; i <= 20; i++) {
            BlockedLongs v = new BlockedLongs(path, i);
            long pos1 = v.allocate();
            long pos2 = v.allocate();
            assertEquals(0, pos1);
            assertEquals(16 + (8 * i), pos2); // brittle
            v.clear();
        }
    }

    @Test
    public void testAppend() throws Exception {
        BlockedLongs v = new BlockedLongs(path, 10);
        long pos1 = v.allocate();
        for (long i = 0; i < 20; i++) {
            v.append(pos1, i);
        }
        long pos2 = v.allocate();
        for (long i = 100; i < 120; i++) {
            v.append(pos2, i);
        }
        assertArrayEquals(new long[] {
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
        }, v.values(pos1).toArray());
        assertArrayEquals(new long[] {
                100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119
        }, v.values(pos2).toArray());
    }

    @Test(expected = IllegalStateException.class)
    public void testAppendAtNonStartingBlock() throws Exception {
        BlockedLongs v = new BlockedLongs(path, 10);
        long pos1 = v.allocate();
        for (long i = 0; i < 21; i++) {
            v.append(pos1, i);
        }
        int blockSize = 16 + 10 * 8; // mirrors BlockedLongs.blockSize
        v.append(blockSize * 2, 21);
    }

    @Test
    public void testLastValue() throws Exception {
        BlockedLongs v = new BlockedLongs(path, 4);
        long pos = v.allocate();
        for (long i = 0; i < 257; i++) {
            v.append(pos, i);
        }
        assertEquals(256, v.lastValue(pos));
    }

    @Test
    public void testFlushAndCloseTwice() throws Exception {
        BlockedLongs block = new BlockedLongs(path, 1);
        block.flush();
        block.flush();
        block.close();
        block.close();
    }
}
