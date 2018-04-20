package com.upserve.uppend.blobs;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Objects;

public class PageKey {
    private final Path filePath;
    private final long page;

    PageKey(Path filePath, long page){
        this.filePath = filePath;
        this.page = page;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PageKey pageKey = (PageKey) o;
        return page == pageKey.page &&
                Objects.equals(filePath, pageKey.filePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filePath, page);
    }

    Path getFilePath() {
        return filePath;
    }

    long getPage() {
        return page;
    }

    @Override
    public String toString() {
        return "PageKey{" +
                "filePath=" + filePath +
                ", page=" + page +
                '}';
    }
}