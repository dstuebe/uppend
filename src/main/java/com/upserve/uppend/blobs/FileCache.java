package com.upserve.uppend.blobs;

import com.github.benmanes.caffeine.cache.*;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.concurrent.*;
import java.util.function.Function;

/**
 * A cache of open file handles.
 * If it is desirable to explicitly manage file close - we can add a method to invalidate a path and make all
 * the objects using Files closable but it seems better to just close the cache when everything is done.
 */
public class FileCache implements Flushable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final LoadingCache<Path, FileCacheEntry> fileCache;
    private final boolean readOnly;

    public FileCache(int initialCacheSize, int maximumCacheSize, boolean readOnly){
        this.readOnly = readOnly;
        this.fileCache = Caffeine
                .<Path, FileCacheEntry>newBuilder()
//                .executor(Executors.newCachedThreadPool())
                .initialCapacity(initialCacheSize)
                .maximumSize(maximumCacheSize)
                .expireAfterAccess(1, TimeUnit.HOURS)
                .recordStats()
                .<Path, FileCacheEntry>removalListener((key, value, cause) ->  {
                    log.debug("Called removal on key {}, value {} with cause {}", key, value, cause);
                    if (value == null) return;
                    try {
                        value.close();
                    } catch (IOException e) {
                        log.error("Unable to close file {}", key, e);
                    }

                })
                .<Path, FileCacheEntry>build(path -> {
                    log.debug("opening {} in file cache", path);
                    return FileCacheEntry.open(path, readOnly);
                });
    }

    public boolean readOnly(){
        return readOnly;
    }

    public FileCacheEntry getEntry(Path path){
        return fileCache.get(path);
    }

    public FileCacheEntry getEntryIfPresent(Path path) { return fileCache.getIfPresent(path); }

    @Override
    public void flush(){
        fileCache.invalidateAll();
    }

    public CacheStats stats(){
        return fileCache.stats();
    }
}
