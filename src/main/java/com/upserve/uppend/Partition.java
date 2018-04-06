package com.upserve.uppend;

import com.google.common.collect.Maps;
import com.upserve.uppend.blobs.*;
import com.upserve.uppend.lookup.*;
import org.slf4j.Logger;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.file.*;
import java.util.Map;
import java.util.function.*;
import java.util.stream.Stream;

public class Partition implements Flushable {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Path partitiondDir;
    private final LongLookup lookups;
    private final Blobs blobs;

    public static Path lookupsDir(Path partitiondDir){
        return partitiondDir.resolve("lookups");
    }

    public static Path blobsFile(Path partitiondDir){
        return partitiondDir.resolve("blobs");
    }

    public static Stream<String> listPartitions(Path partitiondPath){

        try {
            return Files.list(partitiondPath).filter(path -> Files.exists(blobsFile(path))).map(path -> path.toFile().getName());
        } catch (IOException e){
            log.error("Unable to list partitions in")
        }
    }


    public static Partition createPartition(Path partentDir, String partition, int hashSize, PagedFileMapper blobPageCache, LookupCache lookupCache){
        Partition.validatePartition(partition);
        Path partitiondDir = partentDir.resolve(partition);
        return new Partition(
                partitiondDir,
                new LongLookup(lookupsDir(partitiondDir), hashSize, PartitionLookupCache.create(partition, lookupCache)),
                new Blobs(blobsFile(partitiondDir), blobPageCache)
                );
    }

    public static Partition openPartition(Path partentDir, String partition, int hashSize, PagedFileMapper blobPageCache, LookupCache lookupCache) {
        Partition.validatePartition(partition);
        Path partitiondDir = partentDir.resolve(partition);
        Path blobsFile = blobsFile(partitiondDir);
        Path lookupsDir = lookupsDir(partitiondDir);

        if (Files.exists(blobsFile)) {
            return new Partition(
                    partitiondDir,
                    new LongLookup(lookupsDir, hashSize, PartitionLookupCache.create(partition, lookupCache)),
                    new Blobs(blobsFile, blobPageCache)
            );
        } else {
            return null;
        }
    }

    private Partition(Path partentDir, LongLookup longLookup, Blobs blobs){
        partitiondDir = partentDir;
        this.lookups = longLookup;
        this.blobs = blobs;
    }

    void append(String key, byte[] blob, BlockedLongs blocks){
        long blobPos = blobs.append(blob);
        long blockPos = lookups.putIfNotExists(key, blocks::allocate);
        blocks.append(blockPos, blobPos);
        log.trace("appending {} bytes (blob pos {}, block pos {}) for path '{}', key '{}'", blob.length, blobPos, blockPos, partitiondDir, key);
    }

    Stream<byte[]> read(String key, BlockedLongs blocks){
        // Consider sorting by blob pos or even grouping by the page of the blob pos and then flat-mapping the reads by page.
        return blocks.values(lookups.getLookupData(key)).parallel().mapToObj(blobs::read);
    }

    Stream<byte[]> readSequential(String key, BlockedLongs blocks){
        // Consider sorting by blob pos or even grouping by the page of the blob pos and then flat-mapping the reads by page.
        return blocks.values(lookups.getLookupData(key)).mapToObj(blobs::read);
    }

    public byte[] readLast(String key, BlockedLongs blocks) {
        return blobs.read(blocks.lastValue(lookups.getLookupData(key)));
    }

    Stream<Map.Entry<String, Stream<byte[]>>> scan(BlockedLongs blocks){
        return lookups.scan()
                .map(entry -> Maps.immutableEntry(
                        entry.getKey(),
                        blocks.values(entry.getValue()).mapToObj(blobs::read)
                ));
    }

    void scan(BlockedLongs blocks, BiConsumer<String, Stream<byte[]>> callback){
        lookups.scan((s, value) -> callback.accept(s, blocks.values(value).mapToObj(blobs::read)));
    }

    Stream<String> keys(){
        return lookups.keys();
    }

    @Override
    public void flush() throws IOException {
        lookups.flush();
        blobs.flush();
    }

    void clear(){
        lookups.clear();
        blobs.clear();
    }

    public static void validatePartition(String partition) {
        if (partition == null) {
            throw new NullPointerException("null partition");
        }
        if (partition.isEmpty()) {
            throw new IllegalArgumentException("empty partition");
        }

        if (!isValidPartitionCharStart(partition.charAt(0))) {
            throw new IllegalArgumentException("bad first-char of partition: " + partition);
        }

        for (int i = partition.length() - 1; i > 0; i--) {
            if (!isValidPartitionCharPart(partition.charAt(i))) {
                throw new IllegalArgumentException("bad char at position " + i + " of partition: " + partition);
            }
        }
    }

    private static boolean isValidPartitionCharStart(char c) {
        return Character.isJavaIdentifierPart(c);
    }

    private static boolean isValidPartitionCharPart(char c) {
        return Character.isJavaIdentifierPart(c) || c == '-';
    }

}
