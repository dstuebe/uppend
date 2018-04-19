package com.upserve.uppend.blobs;


import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

public class FileCacheEntry implements Closeable{

    final FileChannel fileChannel;
    final AtomicLong fileSize;

    private static final OpenOption[] readOnlyOpenOptions = new OpenOption[]{StandardOpenOption.READ};
    private static final OpenOption[] readWriteOpenOptions = new OpenOption[]{StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE};

    public static FileCacheEntry open(Path path, boolean readOnly) throws IOException {
        final FileChannel fileChannel;
        final long size;
        if (readOnly) {
            fileChannel = FileChannel.open(path, readOnlyOpenOptions);
            size = -1;
        } else {
            fileChannel = FileChannel.open(path, readWriteOpenOptions);
            size = fileChannel.size();
        }

        return new FileCacheEntry(fileChannel, size);
    }

    private FileCacheEntry(FileChannel fileChannel, long fileSize) {
        this.fileChannel = fileChannel;
        this.fileSize = new AtomicLong(fileSize);
    }

    @Override
    public void close() throws IOException {
        if (fileChannel != null && fileChannel.isOpen()) fileChannel.close();
    }
}
