package com.upserve.uppend;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.upserve.uppend.lookup.*;
import com.upserve.uppend.util.SafeDeleting;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.function.*;
import java.util.stream.Stream;

public class FileCounterStore extends FileStore<CounterStorePartition> implements CounterStore {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final LookupCache lookupCache;
    private final Function<String, CounterStorePartition> openPartitionFunction;
    private final Function<String, CounterStorePartition> createPartitionFunction;

    FileCounterStore(boolean readOnly, CounterStoreBuilder builder) {
        super(builder.getDir(), builder.getFlushDelaySeconds(), builder.getPartitionSize(), readOnly, builder.getStoreName());

        lookupCache = builder.buildLookupCache(getName(), readOnly);

        openPartitionFunction = partitionKey -> CounterStorePartition.openPartition(partitionsDir, partitionKey, builder.getLookupHashSize(), builder.getTargetBufferSize(), builder.getFlushThreshold(), builder.getMetadataTTL(), builder.getMetadataPageSize(), builder.getLookupPageSize(), lookupCache, readOnly);
        createPartitionFunction = partitionKey -> CounterStorePartition.createPartition(partitionsDir, partitionKey, builder.getLookupHashSize(), builder.getTargetBufferSize(), builder.getFlushThreshold(), builder.getMetadataTTL(), builder.getMetadataPageSize(), builder.getLookupPageSize(), lookupCache);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Long set(String partitionEntropy, String key, long value) {
        log.trace("setting {}={} in partition '{}'", key, value, partitionEntropy);
        if (readOnly) throw new RuntimeException("Can not set value of counter store opened in read only mode:" + dir);
        return getOrCreate(partitionEntropy).set(key, value);
    }

    @Override
    public long increment(String partitionEntropy, String key, long delta) {
        log.trace("incrementing by {} key '{}' in partition '{}'", delta, key, partitionEntropy);
        if (readOnly)
            throw new RuntimeException("Can not increment value of counter store opened in read only mode:" + dir);
        return getOrCreate(partitionEntropy).increment(key, delta);
    }

    @Override
    public Long get(String partitionEntropy, String key) {
        log.trace("getting value for key '{}' in partition '{}'", key, partitionEntropy);
        return getIfPresent(partitionEntropy).map(partitionObject -> partitionObject.get(key)).orElse(null);
    }

    @Override
    public Stream<String> keys() {
        log.trace("getting keys in {}", getName());
        return streamPartitions()
                .flatMap(CounterStorePartition::keys);
    }

    @Override
    public Stream<Map.Entry<String, Long>> scan() {
        return streamPartitions()
                .flatMap(CounterStorePartition::scan);
    }

    @Override
    public void scan(ObjLongConsumer<String> callback) {
        streamPartitions()
                .forEach(partitionObject -> partitionObject.scan(callback));
    }

    @Override
    public FlushStats getFlushStats() {
        return lookupCache.getFlushStats();
    }

    @Override
    public CacheStats getLookupKeyCacheStats() {
        return lookupCache.keyStats();
    }

    @Override
    public long keyCount() {
        return streamPartitions()
                .mapToLong(CounterStorePartition::keyCount)
                .sum();
    }

    @Override
    public void clear() {
        log.trace("clearing");
        if (readOnly) throw new RuntimeException("Can not clear a store opened in read only mode:" + dir);

        log.trace("clearing");

        partitionMap.values().stream().forEach(counterStorePartition -> {
            try {
                counterStorePartition.clear();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to clear counter store partition", e);
            }
        });

        try {
            SafeDeleting.removeDirectory(partitionsDir);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to clear partitions directory", e);
        }
        partitionMap.clear();
        lookupCache.flush();
    }

    @Override
    public void trimInternal() {
        if (!readOnly) flushInternal();
        lookupCache.flush();
    }

    @Override
    Function<String, CounterStorePartition> getOpenPartitionFunction() {
        return openPartitionFunction;
    }

    @Override
    Function<String, CounterStorePartition> getCreatePartitionFunction() {
        return createPartitionFunction;
    }

    @Override
    protected void flushInternal() {
        if (readOnly) throw new RuntimeException("Can not flush a store opened in read only mode:" + dir);

        partitionMap.values().parallelStream().forEach(CounterStorePartition::flush);
    }

    @Override
    protected void closeInternal() {
        partitionMap.values().parallelStream().forEach(counterStorePartition -> {
            try {
                counterStorePartition.close();
            } catch (IOException e) {
                throw new UncheckedIOException("Error closing store " + dir, e);
            }
        });

        lookupCache.flush();
    }
}
