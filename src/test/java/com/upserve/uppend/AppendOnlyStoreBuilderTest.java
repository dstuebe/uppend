package com.upserve.uppend;

import com.codahale.metrics.MetricRegistry;
import com.upserve.uppend.metrics.AppendOnlyStoreWithMetrics;
import com.upserve.uppend.util.SafeDeleting;
import org.junit.Test;

import java.nio.file.*;

import static com.upserve.uppend.metrics.AppendOnlyStoreWithMetrics.FLUSH_TIMER_METRIC_NAME;
import static com.upserve.uppend.metrics.AppendOnlyStoreWithMetrics.getFullMetricName;
import static org.junit.Assert.assertEquals;

public class AppendOnlyStoreBuilderTest {
    @Test
    public void testBuildWithMetrics() throws Exception {
        Path path = Paths.get("build/tmp/test/append-only-store-builder");
        SafeDeleting.removeDirectory(path);
        MetricRegistry metrics = new MetricRegistry();
        AppendOnlyStore store = Uppend.store(path).withStoreMetrics(metrics).build(false);
        store.flush();
        assertEquals(1, metrics.getTimers().get(getFullMetricName(store, FLUSH_TIMER_METRIC_NAME)).getCount());
    }
}
