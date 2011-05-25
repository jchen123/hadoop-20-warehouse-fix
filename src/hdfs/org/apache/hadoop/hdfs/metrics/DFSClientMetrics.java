package org.apache.hadoop.hdfs.metrics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingLong;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

public class DFSClientMetrics implements Updater {
  public MetricsRegistry registry = new MetricsRegistry();
  public MetricsTimeVaryingRate lsLatency = new MetricsTimeVaryingRate(
      "client.ls.latency", registry,
      "The time taken by DFSClient to perform listStatus");
  public MetricsTimeVaryingLong readsFromLocalFile = new MetricsTimeVaryingLong(
      "client.read.localfile", registry,
      "The number of time read is fetched directly from local file.");
        
  private long numLsCalls = 0;
  private static Log log = LogFactory.getLog(DFSClientMetrics.class);
  final MetricsRecord metricsRecord;

  public DFSClientMetrics() {
    // Create a record for FSNamesystem metrics
    MetricsContext metricsContext = MetricsUtil.getContext("hdfsclient");
    metricsRecord = MetricsUtil.createRecord(metricsContext, "DFSClient");
    metricsContext.registerUpdater(this);
  }


  public synchronized void incLsCalls() {
    numLsCalls++;
  }
  
  public synchronized long getAndResetLsCalls() {
    long ret = numLsCalls;
    numLsCalls = 0;
    return ret;
  }
  /**
   * Since this object is a registered updater, this method will be called
   * periodically, e.g. every 5 seconds.
   */
  public void doUpdates(MetricsContext unused) {
    synchronized (this) {
      for (MetricsBase m : registry.getMetricsList()) {
        m.pushMetric(metricsRecord);
      }
    }
    metricsRecord.setMetric("client.ls.calls", getAndResetLsCalls());
    metricsRecord.update();
  }
}
