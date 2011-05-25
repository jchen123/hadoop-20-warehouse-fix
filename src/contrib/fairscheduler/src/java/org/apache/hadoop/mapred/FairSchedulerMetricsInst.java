package org.apache.hadoop.mapred;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FairScheduler.JobInfo;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;


public class FairSchedulerMetricsInst implements Updater {

  private final FairScheduler scheduler;
  private final MetricsRecord metricsRecord;
  private final JobTracker jobTracker;
  private final MetricsContext context = MetricsUtil.getContext("mapred");
  private final Map<String, MetricsRecord> poolToMetricsRecord;

  private long updatePeriod = 0;
  private long lastUpdateTime = 0;

  private int numPreemptMaps = 0;
  private int numPreemptReduces = 0;

  public FairSchedulerMetricsInst(FairScheduler scheduler, Configuration conf) {
    this.scheduler = scheduler;
    this.jobTracker = (JobTracker) scheduler.taskTrackerManager;
    // Create a record for map-reduce metrics
    metricsRecord = MetricsUtil.createRecord(context, "fairscheduler");
    poolToMetricsRecord = new HashMap<String, MetricsRecord>();
    context.registerUpdater(this);

    updatePeriod = conf.getLong("mapred.fairscheduler.metric.update.period",
                                5 * 60 * 1000);  // default period is 5 MINs
  }

  @Override
  public void doUpdates(MetricsContext context) {
    long now = JobTracker.getClock().getTime();
    if (now - lastUpdateTime > updatePeriod) {
      updateMetrics();
      lastUpdateTime = now;
    }
    updateCounters();
    metricsRecord.update();
    for (MetricsRecord mr : poolToMetricsRecord.values()) {
      mr.update();
    }
  }

  public synchronized void preemptMap(TaskAttemptID taskAttemptID) {
    ++numPreemptMaps;
  }

  public synchronized void preemptReduce(TaskAttemptID taskAttemptID) {
    ++numPreemptReduces;
  }

  private void updateCounters() {
    synchronized (this) {
      metricsRecord.incrMetric("num_preempt_maps", numPreemptMaps);
      metricsRecord.incrMetric("num_preempt_reduces", numPreemptReduces);

      numPreemptMaps = 0;
      numPreemptReduces = 0;
    }
  }

  private void updateMetrics() {

    int numActivePools = 0;
    int numStarvedPools = 0;
    int numStarvedJobs = 0;
    int totalRunningMaps = 0;
    int totalRunningReduces = 0;
    int totalMinReduces = 0;
    int totalMaxReduces = 0;
    int totalMinMaps = 0;
    int totalMaxMaps = 0;
    int totalRunningJobs = 0;

    Collection<PoolInfo> infos = new LinkedList<PoolInfo>();
    synchronized (jobTracker) {
      synchronized(scheduler) {
        PoolManager poolManager = scheduler.getPoolManager();
        for (Pool pool: poolManager.getPools()) {
          PoolInfo info = new PoolInfo(pool);
          infos.add(info);
          numStarvedJobs += info.numStarvedJobs;
          totalRunningJobs += info.runningJobs;
          totalRunningMaps += info.runningMaps;
          totalRunningReduces += info.runningReduces;
          totalMinMaps += info.minMaps;
          totalMinReduces += info.minReduces;
          if (info.maxMaps != Integer.MAX_VALUE) {
            totalMaxMaps += info.maxMaps;
          }
          if (info.maxReduces != Integer.MAX_VALUE) {
            totalMaxReduces += info.maxReduces;
          }
          if (info.isActive()) {
            ++numActivePools;
          }
          if (info.isStarved()) {
            ++numStarvedPools;
          }
        }
      }
    }

    for (PoolInfo info : infos) {
      submitPoolMetrics(info);
    }

    metricsRecord.setMetric("num_active_pools", numActivePools);
    metricsRecord.setMetric("num_starved_pools", numStarvedPools);
    metricsRecord.setMetric("num_starved_jobs", numStarvedJobs);
    metricsRecord.setMetric("num_running_jobs", totalRunningJobs);
    metricsRecord.setMetric("total_min_maps", totalMinMaps);
    metricsRecord.setMetric("total_max_maps", totalMaxMaps);
    metricsRecord.setMetric("total_min_reduces", totalMinReduces);
    metricsRecord.setMetric("total_max_reduces", totalMaxReduces);
  }

  private void submitPoolMetrics(PoolInfo info) {
    String pool = info.poolName.toLowerCase();
    MetricsRecord record = poolToMetricsRecord.get(pool);
    if (record == null) {
      record = MetricsUtil.createRecord(context, "pool-" + pool);
      FairScheduler.LOG.info("Create metrics record for pool:" + pool);
      poolToMetricsRecord.put(pool, record);
    }
    record.setMetric("min_map", info.minMaps);
    record.setMetric("min_reduce", info.minReduces);
    record.setMetric("max_map", info.maxMaps);
    record.setMetric("max_reduce", info.maxReduces);
    record.setMetric("running_map", info.runningMaps);
    record.setMetric("running_reduce", info.runningReduces);
    record.setMetric("runnable_map", info.runnableMaps);
    record.setMetric("runnable_reduce", info.runnableReduces);
  }

  private class PoolInfo {

    final String poolName;
    final int runningJobs;
    final int minMaps;
    final int minReduces;
    final int maxMaps;
    final int maxReduces;
    int runningMaps = 0;
    int runningReduces = 0;
    int runnableMaps = 0;
    int runnableReduces = 0;
    int numStarvedJobs = 0;

    PoolInfo(Pool pool) {
      PoolManager poolManager = scheduler.getPoolManager();
      poolName = pool.getName();
      runningJobs = pool.getJobs().size();
      minMaps = poolManager.getAllocation(poolName, TaskType.MAP);
      minReduces = poolManager.getAllocation(poolName, TaskType.REDUCE);
      maxMaps = poolManager.getMaxSlots(poolName, TaskType.MAP);
      maxReduces = poolManager.getMaxSlots(poolName, TaskType.REDUCE);
      countTasks(pool);
    }

    private void countTasks(Pool pool) {
      for (JobInProgress job: pool.getJobs()) {
        JobInfo info = scheduler.infos.get(job);
        if (info != null) {
          runningMaps += info.runningMaps;
          runningReduces += info.runningReduces;
          runnableMaps += info.neededMaps + info.runningMaps;
          runnableReduces += info.neededReduces + info.runningReduces;
          if (isStarvedJob(info)) {
            ++numStarvedJobs;
          }
        }
      }
    }

    private boolean isStarvedJob(JobInfo info) {
      return ((info.neededMaps + info.runningMaps > info.mapFairShare &&
          info.runningMaps < info.mapFairShare) ||
          (info.neededReduces + info.runningReduces > info.reduceFairShare &&
              info.runningReduces < info.reduceFairShare));
    }

    boolean isActive() {
      return !(runningJobs == 0 && minMaps == 0 && minReduces == 0 &&
          maxMaps == Integer.MAX_VALUE && maxReduces == Integer.MAX_VALUE &&
          runningMaps == 0 && runningReduces == 0);
    }

    boolean isStarved() {
      return ((runnableMaps > minMaps && runningMaps < minMaps) ||
              (runnableReduces > minReduces && runningReduces < minReduces));
    }
  }
}
