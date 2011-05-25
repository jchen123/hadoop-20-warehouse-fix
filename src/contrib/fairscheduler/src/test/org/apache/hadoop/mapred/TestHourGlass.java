/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.TestTaskFail.MapperClass;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.TaskType;
import org.junit.Test;



public class TestHourGlass {
  
  final static int FAIR_SCHEDULER_PORT[] = {52823, 52824};
  private static MiniDFSCluster createDFSCluster() throws Exception {
    MiniDFSCluster dfs = new MiniDFSCluster(new Configuration(), 1, true, null);
    dfs.waitActive();
    return dfs;
  }
  
  private static MiniMRCluster createMRCluster(int fairSchedulerPort,
      int maxMaps, int maxReduces, MiniDFSCluster dfs) throws Exception {
    // create a dfs and map-reduce cluster
    JobConf conf = new JobConf();
    // Use FairScheduler
    conf.set("mapred.jobtracker.taskScheduler",
        "org.apache.hadoop.mapred.FairScheduler");
    // Use the correct port
    conf.set("mapred.fairscheduler.server.address",
        "localhost:" + fairSchedulerPort);
    // Set maximum tasks
    conf.setInt("mapred.fairscheduler.map.tasks.maximum", maxMaps);
    conf.setInt("mapred.fairscheduler.reduce.tasks.maximum", maxReduces);
    conf.setInt("mapred.fairscheduler.reduce.tasks.maximum", maxReduces);
    // Set maximum tasks on TT to be larger so FS can take control
    conf.setInt("mapred.tasktracker.map.tasks.maximum", 10);
    conf.setInt("mapred.tasktracker.reduce.tasks.maximum", 10);
    FileSystem fileSys = dfs.getFileSystem();
    String namenode = fileSys.getUri().toString();
    // Run only one tasktracker
    final int taskTrackers = 1;
    final String racks[] = {"/rack1"};
    final String hosts[] = {"host1.rack1.com"};
    MiniMRCluster mr = new MiniMRCluster(taskTrackers, namenode, 3, racks, hosts, conf);
    return mr;
  }
  
  private static HourGlass createHourGlass() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(HourGlass.MAX_MAP_KEY, 2);
    conf.setInt(HourGlass.MAX_REDUCE_KEY, 3);
    conf.setLong(HourGlass.INTERVAL_KEY, 1000L);
    conf.set(HourGlass.SERVERS_KEY, String.format("localhost:%s,localhost:%s",
        FAIR_SCHEDULER_PORT[0], FAIR_SCHEDULER_PORT[1]));
    conf.set(HourGlass.WEIGHTS_KEY, "1.0, 1.0");
    return new HourGlass(conf);
  }
  private static  RunningJob launchJob(JobConf conf, Path inDir, Path outDir,
      String input)  throws IOException {
    // set up the input file system and write input text.
    FileSystem inFs = inDir.getFileSystem(conf);
    FileSystem outFs = outDir.getFileSystem(conf);
    outFs.delete(outDir, true);
    if (!inFs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    {
      // write input into input file
      DataOutputStream file = inFs.create(new Path(inDir, "part-0"));
      file.writeBytes(input);
      file.close();
    }

    // configure the mapred Job
    conf.setMapperClass(MapperClass.class);        
    conf.setReducerClass(IdentityReducer.class);
    conf.setNumReduceTasks(1);
    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    conf.setSpeculativeExecution(false);
    final String TEST_ROOT_DIR = new Path(System.getProperty("test.build.data",
                                    "/tmp")).toString().replace(' ', '+');
    conf.set("test.build.data", TEST_ROOT_DIR);
    // return the RunningJob handle.
    return new JobClient(conf).submitJob(conf);
  }

  @Test
  public void testStartNewCluster() throws Exception {
    MiniMRCluster clusters[] = new MiniMRCluster[2];
    MiniDFSCluster dfs = null;
    HourGlass hourGlass = null;
    final Path inDir = new Path("./input");
    final Path outDir = new Path("./output");
    String input = "The quick brown fox\nhas many silly\nred fox sox\n";
    try {
      dfs = createDFSCluster();
      clusters[0] = createMRCluster(FAIR_SCHEDULER_PORT[0], 2, 3, dfs);
      clusters[1] = createMRCluster(FAIR_SCHEDULER_PORT[1], 0, 0, dfs);
      FairScheduler fairSchedulers[] =
          {(FairScheduler)clusters[0].getJobTrackerRunner().getJobTracker().getTaskScheduler(), 
           (FairScheduler)clusters[1].getJobTrackerRunner().getJobTracker().getTaskScheduler()};
      String taskTrackerNames[] =
          {clusters[0].getTaskTrackerRunner(0).getTaskTracker().getName(),
           clusters[1].getTaskTrackerRunner(0).getTaskTracker().getName()};
      // Submit job to cluster 1 which has no slots
      JobConf jobConf = clusters[1].createJobConf();
      RunningJob rJob = launchJob(jobConf, inDir, outDir, input);
      hourGlass = createHourGlass();
      Thread hourGlassThread = new Thread(hourGlass);
      hourGlassThread.start();
      rJob.waitForCompletion();
      // Verify that all slots have been moved
      Assert.assertEquals(0, fairSchedulers[0].getFSMaxSlots(taskTrackerNames[0], TaskType.MAP));
      Assert.assertEquals(0, fairSchedulers[0].getFSMaxSlots(taskTrackerNames[0], TaskType.REDUCE));
      Assert.assertEquals(2, fairSchedulers[1].getFSMaxSlots(taskTrackerNames[1], TaskType.MAP));
      Assert.assertEquals(3, fairSchedulers[1].getFSMaxSlots(taskTrackerNames[1], TaskType.REDUCE));
    } finally {
      if (dfs != null) {
        dfs.shutdown();
      }
      if (clusters[0] != null) {
        clusters[0].shutdown();
      }
      if (clusters[1] != null) {
        clusters[1].shutdown();
      }
    }
  }
}
