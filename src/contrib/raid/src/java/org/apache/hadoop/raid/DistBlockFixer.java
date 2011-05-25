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

package org.apache.hadoop.raid;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.tools.DFSck;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;

/**
 * distributed block fixer, uses map reduce jobs to fix corrupt files
 *
 * configuration options
 * raid.blockfix.filespertask       - number of corrupt files to fix in a single
 *                                    map reduce task (i.e., at one mapper node)
 *
 * raid.blockfix.fairscheduler.pool - the pool to use for block fixer jobs
 *
 * raid.blockfix.maxpendingjobs    - maximum number of block fixer jobs
 *                                    running simultaneously
 */
public class DistBlockFixer extends BlockFixer {
  // volatile should be sufficient since only the block fixer thread
  // updates numJobsRunning (other threads may read)
  private volatile int numJobsRunning = 0;

  private static final String WORK_DIR_PREFIX = "blockfixer";
  private static final String IN_FILE_SUFFIX = ".in";
  private static final String PART_PREFIX = "part-";
  private static final Pattern LIST_CORRUPT_FILE_PATTERN =
      Pattern.compile("blk_-*\\d+\\s+(.*)");
  
  private static final String BLOCKFIX_FILES_PER_TASK = 
    "raid.blockfix.filespertask";
  private static final String BLOCKFIX_MAX_PENDING_JOBS =
    "raid.blockfix.maxpendingjobs";
  private static final String HIGH_PRI_SCHEDULER_OPTION =
    "raid.blockfix.highpri.scheduleroption";
  private static final String LOW_PRI_SCHEDULER_OPTION =
    "raid.blockfix.lowpri.scheduleroption";
  private static final String MAX_FIX_TIME_FOR_FILE =
    "raid.blockfix.max.fix.time.for.file";

  // default number of files to fix in a task
  private static final long DEFAULT_BLOCKFIX_FILES_PER_TASK = 10L;

  private static final int BLOCKFIX_TASKS_PER_JOB = 50;

  // default number of files to fix simultaneously
  private static final long DEFAULT_BLOCKFIX_MAX_PENDING_JOBS = 100L;

  private static final long DEFAULT_MAX_FIX_FOR_FILE =
    4 * 60 * 60 * 1000;  // 4 hrs.
 
  protected static final Log LOG = LogFactory.getLog(DistBlockFixer.class);

  // number of files to fix in a task
  private long filesPerTask;

  // number of files to fix simultaneously
  final private long maxPendingJobs;

  final private long maxFixTimeForFile;

  private final SimpleDateFormat dateFormat =
    new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");

  private Map<String, CorruptFileInfo> fileIndex = 
    new HashMap<String, CorruptFileInfo>();
  private Map<Job, List<CorruptFileInfo>> jobIndex =
    new HashMap<Job, List<CorruptFileInfo>>();

  private long jobCounter = 0;
  private volatile BlockFixer.Status lastStatus = null;

  static enum Counter {
    FILES_SUCCEEDED, FILES_FAILED, FILES_NOACTION
  }

  public DistBlockFixer(Configuration conf) {
    super(conf);
    filesPerTask = DistBlockFixer.filesPerTask(getConf());
    maxPendingJobs = DistBlockFixer.maxPendingJobs(getConf());
    maxFixTimeForFile = DistBlockFixer.maxFixTimeForFile(getConf());
  }

  /**
   * determines how many files to fix in a single task
   */ 
  protected static long filesPerTask(Configuration conf) {
    return conf.getLong(BLOCKFIX_FILES_PER_TASK, 
                        DEFAULT_BLOCKFIX_FILES_PER_TASK);

  }
  /**
   * determines how many files to fix simultaneously
   */ 
  protected static long maxPendingJobs(Configuration conf) {
    return conf.getLong(BLOCKFIX_MAX_PENDING_JOBS,
                        DEFAULT_BLOCKFIX_MAX_PENDING_JOBS);
  }

  protected static long maxFixTimeForFile(Configuration conf) {
    return conf.getLong(MAX_FIX_TIME_FOR_FILE,
                        DEFAULT_MAX_FIX_FOR_FILE);
  }

  /**
   * runs the block fixer periodically
   */
  public void run() {
    while (running) {
      try {
        checkAndFixBlocks();
        updateStatus();
      } catch (InterruptedException ignore) {
        LOG.info("interrupted");
      } catch (Exception e) {
        // log exceptions and keep running
        LOG.error(StringUtils.stringifyException(e));
      } catch (Error e) {
        LOG.error(StringUtils.stringifyException(e));
        throw e;
      }

      try {
        Thread.sleep(blockFixInterval);
      } catch (InterruptedException ignore) {
        LOG.info("interrupted");
      }
    }
  }

  /**
   * checks for corrupt blocks and fixes them (if any)
   */
  void checkAndFixBlocks()
    throws IOException, InterruptedException, ClassNotFoundException {
    checkJobs();

    if (jobIndex.size() >= maxPendingJobs) {
      LOG.info("Waiting for " + jobIndex.size() + " pending jobs");
      return;
    }

    Map<String, Integer> corruptFiles = getCorruptFiles();
    FileSystem fs = new Path("/").getFileSystem(getConf());
    Map<String, Integer> corruptFilePriority =
      computePriorities(fs, corruptFiles);

    int totalCorruptFiles =
      corruptFilePriority.size() + fileIndex.size();
    RaidNodeMetrics.getInstance().numFilesToFix.set(totalCorruptFiles);

    String startTimeStr = dateFormat.format(new Date());

    LOG.info("Found " + corruptFilePriority.size() + " corrupt files");

    if (corruptFilePriority.size() > 0) {
      for (int pri = 1; pri >= 0; pri--) {
        String jobName = "blockfixer." + jobCounter +
          ".pri" + pri + "." + startTimeStr;
        jobCounter++;
        startJob(jobName, corruptFilePriority, pri);
      }
    }
  }

  // Compute integer priority. Urgency is indicated by higher numbers.
  Map<String, Integer> computePriorities(
      FileSystem fs, Map<String, Integer> corruptFiles) throws IOException {

    Map<String, Integer> corruptFilePriority = new HashMap<String, Integer>();
    String[] parityDestPrefixes = destPrefixes();
    Set<String> srcDirsToWatchOutFor = new HashSet<String>();
    // Loop over parity files once.
    for (Iterator<String> it = corruptFiles.keySet().iterator(); it.hasNext(); ) {
      String p = it.next();
      if (BlockFixer.isSourceFile(p, parityDestPrefixes)) {
        continue;
      }
      // Find the parent of the parity file.
      Path parent = new Path(p).getParent();
      // If the file was a HAR part file, the parent will end with _raid.har. In
      // that case, the parity directory is the parent of the parent.
      if (parent.toUri().getPath().endsWith(RaidNode.HAR_SUFFIX)) {
        parent = parent.getParent();
      }
      String parentUriPath = parent.toUri().getPath();
      // Remove the RAID prefix to get the source dir.
      srcDirsToWatchOutFor.add(
        parentUriPath.substring(parentUriPath.indexOf(Path.SEPARATOR, 1)));
      int numCorrupt = corruptFiles.get(p);
      int priority = (numCorrupt > 1) ? 1 : 0;
      CorruptFileInfo fileInfo = fileIndex.get(p);
      if (fileInfo == null || priority > fileInfo.getHighestPriority()) {
        corruptFilePriority.put(p, priority);
      }
    }
    // Loop over src files now.
    for (Iterator<String> it = corruptFiles.keySet().iterator(); it.hasNext(); ) {
      String p = it.next();
      if (BlockFixer.isSourceFile(p, parityDestPrefixes)) {
        if (BlockFixer.doesParityDirExist(fs, p, parityDestPrefixes)) {
          int numCorrupt = corruptFiles.get(p);
          FileStatus stat = fs.getFileStatus(new Path(p));
          int priority = 0;
          if (stat.getReplication() > 1) {
            // If we have a missing block when replication > 1, it is high pri.
            priority = 1;
          } else {
            // Replication == 1. Assume Reed Solomon parity exists.
            // If we have more than one missing block when replication == 1, then
            // high pri.
            priority = (numCorrupt > 1) ? 1 : 0;
          }
          // If priority is low, check if the scan of corrupt parity files found
          // the src dir to be risky.
          if (priority == 0) {
            Path parent = new Path(p).getParent();
            String parentUriPath = parent.toUri().getPath();
            if (srcDirsToWatchOutFor.contains(parentUriPath)) {
              priority = 1;
            }
          }
          CorruptFileInfo fileInfo = fileIndex.get(p);
          if (fileInfo == null || priority > fileInfo.getHighestPriority()) {
            corruptFilePriority.put(p, priority);
          }
        }
      }
    }
    return corruptFilePriority;
  }

  /**
   * Handle a failed job.
   */
  private void failJob(Job job) throws IOException {
    // assume no files have been fixed
    LOG.error("Job " + job.getID() + "(" + job.getJobName() +
      ") finished (failed)");
    // We do not change metrics here since we do not know for sure if file
    // fixing failed.
    for (CorruptFileInfo fileInfo: jobIndex.get(job)) {
      boolean failed = true;
      fileInfo.finishJob(job.getJobName(), failed);
    }
    numJobsRunning--;
  }

  /**
   * Handle a successful job.
   */
  private void succeedJob(Job job, long filesSucceeded, long filesFailed)
    throws IOException {
    String jobName = job.getJobName();
    LOG.info("Job " + job.getID() + "(" + jobName +
      ") finished (succeeded)");

    if (filesFailed == 0) {
      // no files have failed
      for (CorruptFileInfo fileInfo: jobIndex.get(job)) {
        boolean failed = false;
        fileInfo.finishJob(jobName, failed);
      }
    } else {
      // we have to look at the output to check which files have failed
      Set<String> failedFiles = getFailedFiles(job);

      for (CorruptFileInfo fileInfo: jobIndex.get(job)) {
        if (failedFiles.contains(fileInfo.getFile().toString())) {
          boolean failed = true;
          fileInfo.finishJob(jobName, failed);
        } else {
          // call succeed for files that have succeeded or for which no action
          // was taken
          boolean failed = false;
          fileInfo.finishJob(jobName, failed);
        }
      }
    }
    // report succeeded files to metrics
    incrFilesFixed(filesSucceeded);
    incrFileFixFailures(filesFailed);
    numJobsRunning--;
  }

  /**
   * checks if jobs have completed and updates job and file index
   * returns a list of failed files for restarting
   */
  void checkJobs() throws IOException {
    Iterator<Job> jobIter = jobIndex.keySet().iterator();
    while(jobIter.hasNext()) {
      Job job = jobIter.next();

      try {
        if (job.isComplete()) {
          long slotSeconds = job.getCounters().findCounter(
            JobInProgress.Counter.SLOTS_MILLIS_MAPS).getValue() / 1000;
          RaidNodeMetrics.getInstance().blockFixSlotSeconds.inc(slotSeconds);
          long filesSucceeded =
            job.getCounters().findCounter(Counter.FILES_SUCCEEDED) != null ?
            job.getCounters().findCounter(Counter.FILES_SUCCEEDED).getValue():
            0;
          long filesFailed =
            job.getCounters().findCounter(Counter.FILES_FAILED) != null ?
            job.getCounters().findCounter(Counter.FILES_FAILED).getValue():
            0;
          long filesNoAction =
            job.getCounters().findCounter(Counter.FILES_NOACTION) != null ?
            job.getCounters().findCounter(Counter.FILES_NOACTION).getValue():
            0;
          int files = jobIndex.get(job).size();
          if (job.isSuccessful() && 
              (filesSucceeded + filesFailed + filesNoAction == 
               ((long) files))) {
            // job has processed all files
            succeedJob(job, filesSucceeded, filesFailed);
          } else {
            failJob(job);
          }
          jobIter.remove();
        } else {
          LOG.info("Job " + job.getID() + "(" + job.getJobName()
            + " still running");
        }
      } catch (Exception e) {
        LOG.error(StringUtils.stringifyException(e));
        failJob(job);
        try {
          job.killJob();
        } catch (Exception ee) {
          LOG.error(StringUtils.stringifyException(ee));
        }
        jobIter.remove();
      }
    }
    purgeFileIndex();
  }

  /**
   * determines which files have failed for a given job
   */
  private Set<String> getFailedFiles(Job job) throws IOException {
    Set<String> failedFiles = new HashSet<String>();

    Path outDir = SequenceFileOutputFormat.getOutputPath(job);
    FileSystem fs  = outDir.getFileSystem(getConf());
    if (!fs.getFileStatus(outDir).isDir()) {
      throw new IOException(outDir.toString() + " is not a directory");
    }

    FileStatus[] files = fs.listStatus(outDir);

    for (FileStatus f: files) {
      Path fPath = f.getPath();
      if ((!f.isDir()) && (fPath.getName().startsWith(PART_PREFIX))) {
        LOG.info("opening " + fPath.toString());
        SequenceFile.Reader reader = 
          new SequenceFile.Reader(fs, fPath, getConf());

        Text key = new Text();
        Text value = new Text();
        while (reader.next(key, value)) {
          failedFiles.add(key.toString());
        }
        reader.close();
      }
    }
    return failedFiles;
  }


  /**
   * purge expired jobs from the file index
   */
  private void purgeFileIndex() {
    Iterator<String> fileIter = fileIndex.keySet().iterator();
    long now = System.currentTimeMillis();
    while(fileIter.hasNext()) {
      String file = fileIter.next();
      if (fileIndex.get(file).isTooOld(now)) {
        fileIter.remove();
      }
    }
  }

  /**
   * creates and submits a job, updates file index and job index
   */
  private void startJob(String jobName, Map<String, Integer> corruptFilePriority,
      int priority)
      throws IOException, InterruptedException, ClassNotFoundException {
    Path inDir = new Path(WORK_DIR_PREFIX + "/in/" + jobName);
    Path outDir = new Path(WORK_DIR_PREFIX + "/out/" + jobName);
    List<String> filesInJob = createInputFile(
      jobName, inDir, corruptFilePriority, priority);
    if (filesInJob.isEmpty()) return;

    Configuration jobConf = new Configuration(getConf());
    RaidUtils.parseAndSetOptions(
      jobConf,
      priority == 1 ? HIGH_PRI_SCHEDULER_OPTION : LOW_PRI_SCHEDULER_OPTION);
    Job job = new Job(jobConf, jobName);
    job.setJarByClass(getClass());
    job.setMapperClass(DistBlockFixerMapper.class);
    job.setNumReduceTasks(0);
    job.setInputFormatClass(DistBlockFixerInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    DistBlockFixerInputFormat.setInputPaths(job, inDir);
    SequenceFileOutputFormat.setOutputPath(job, outDir);

    submitJob(job, filesInJob, priority);
    List<CorruptFileInfo> fileInfos =
      updateFileIndex(jobName, filesInJob, priority);
    // The implementation of submitJob() need not update jobIndex.
    // So check if the job exists in jobIndex before updating jobInfos.
    if (jobIndex.containsKey(job)) {
      jobIndex.put(job, fileInfos);
    }
    numJobsRunning++;
  }

  // Can be overridded by tests.
  void submitJob(Job job, List<String> filesInJob, int priority)
      throws IOException, InterruptedException, ClassNotFoundException {
    job.submit();
    LOG.info("Job " + job.getID() + "(" + job.getJobName() +
      ") started");
    jobIndex.put(job, null);
  }

  /**
   * inserts new job into file index and job index
   */
  private List<CorruptFileInfo> updateFileIndex(
      String jobName, List<String> corruptFiles, int priority) {
    List<CorruptFileInfo> fileInfos = new ArrayList<CorruptFileInfo>();

    for (String file: corruptFiles) {
      CorruptFileInfo fileInfo = fileIndex.get(file);
      if (fileInfo != null) {
        fileInfo.addJob(jobName, priority);
      } else {
        fileInfo = new CorruptFileInfo(file, jobName, priority);
        fileIndex.put(file, fileInfo);
      }
      fileInfos.add(fileInfo);
    }
    return fileInfos;
  }
 
  /**
   * creates the input file (containing the names of the files to be fixed
   */
  private List<String> createInputFile(String jobName, Path inDir,
                         Map<String, Integer> corruptFilePriority, int priority)
      throws IOException {
    Path file = new Path(inDir, jobName + IN_FILE_SUFFIX);
    FileSystem fs = file.getFileSystem(getConf());
    SequenceFile.Writer fileOut = SequenceFile.createWriter(fs, getConf(), file,
                                                            LongWritable.class,
                                                            Text.class);
    long index = 0L;

    List<String> filesAdded = new ArrayList<String>();
    int count = 0;
    final long max = filesPerTask * BLOCKFIX_TASKS_PER_JOB;
    for (Map.Entry<String, Integer> entry: corruptFilePriority.entrySet()) {
      if (entry.getValue() != priority) {
        continue;
      }
      if (count >= max) {
        break;
      }
      String corruptFileName = entry.getKey();
      fileOut.append(new LongWritable(index++), new Text(corruptFileName));
      filesAdded.add(corruptFileName);
      count++;

      if (index % filesPerTask == 0) {
        fileOut.sync(); // create sync point to make sure we can split here
      }
    }

    fileOut.close();
    return filesAdded;
  }

  /**
   * debugging TODO remove
   */
  private void readOutputFiles(String jobName, Path outDir) throws IOException {
    

    FileSystem fs  = outDir.getFileSystem(getConf());
    if (!fs.getFileStatus(outDir).isDir()) {
      throw new IOException(outDir.toString() + " is not a directory");
    }

    FileStatus[] files = fs.listStatus(outDir);

    for (FileStatus f: files) {
      Path fPath = f.getPath();
      if ((!f.isDir()) && (fPath.getName().startsWith(PART_PREFIX))) {
        LOG.info("opening " + fPath.toString());
        SequenceFile.Reader reader = 
          new SequenceFile.Reader(fs, fPath, getConf());
        Text key = new Text();
        Text value = new Text();
        while (reader.next(key, value)) {
          LOG.info("read " + f.getPath().toString());
          LOG.info("read: k=" + key.toString() + " v=" + value.toString()); 
        }
        LOG.info("done reading " + fPath.toString());

        reader.close();
      }

    }
  }

  /**
   * gets a list of corrupt files from the name node
   * and filters out files that are currently being fixed or 
   * that were recently fixed
   */
  private Map<String, Integer> getCorruptFiles() throws IOException {

    Map<String, Integer> corruptFiles = new HashMap<String, Integer>();
    BufferedReader reader = getCorruptFileReader();
    String line = reader.readLine(); // remove the header line
    while ((line = reader.readLine()) != null) {
      Matcher m = LIST_CORRUPT_FILE_PATTERN.matcher(line);
      if (!m.find()) {
        continue;
      }
      String fileName = m.group(1).trim();
      Integer numCorrupt = corruptFiles.get(fileName);
      numCorrupt = numCorrupt == null ? 0 : numCorrupt;
      numCorrupt += 1;
      corruptFiles.put(fileName, numCorrupt);
    }
    RaidUtils.filterTrash(getConf(), corruptFiles.keySet().iterator());
    return corruptFiles;
  }

  private BufferedReader getCorruptFileReader() throws IOException {

    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(bout, true);
    DFSck dfsck = new DFSck(getConf(), ps);
    try {
      dfsck.run(new String[]{"-list-corruptfileblocks", "-limit", "20000"});
    } catch (Exception e) {
      throw new IOException(e);
    }
    ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
    return new BufferedReader(new InputStreamReader(bin));
  }

  /**
   * returns the number of map reduce jobs running
   */
  public int jobsRunning() {
    return numJobsRunning;
  }

  /**
   * hold information about a corrupt file that is being fixed
   */
  class CorruptFileInfo {

    private String file;
    private List<String> jobNames;  // Jobs fixing this file.
    private boolean done;
    private List<Integer> priorities;
    private long insertTime;

    public CorruptFileInfo(String file, String jobName, int priority) {
      this.file = file;
      this.jobNames = new ArrayList<String>();
      this.priorities = new ArrayList<Integer>();
      this.done = false;
      this.insertTime = System.currentTimeMillis();
      addJob(jobName, priority);
    }

    public boolean isTooOld(long now) {
      return now - insertTime > maxFixTimeForFile;
    }

    public boolean isDone() {
      return done;
    }

    public void addJob(String jobName, int priority) {
      this.jobNames.add(jobName);
      this.priorities.add(priority);
    }

    public int getHighestPriority() {
      int max = 0;
      for (int p: priorities) {
        if (p > max) max = p;
      }
      return max;
    }

    public String getFile() {
      return file;
    }

    /**
     * Updates state with the completion of a job. If all jobs for this file
     * are done, the file index is updated.
     */
    public void finishJob(String jobName, boolean failed) {
      int idx = jobNames.indexOf(jobName);
      if (idx == -1) return;
      jobNames.remove(idx);
      priorities.remove(idx);
      LOG.info("fixing " + file +
        (failed ? " failed in " : " succeeded in ") +
        jobName);
      if (jobNames.isEmpty()) {
        // All jobs dealing with this file are done,
        // remove this file from the index
        CorruptFileInfo removed = fileIndex.remove(file);
        if (removed == null) {
          LOG.error("trying to remove file not in file index: " + file);
        }
        done = true;
      }
    }
  }

  static class DistBlockFixerInputFormat
    extends SequenceFileInputFormat<LongWritable, Text> {

    protected static final Log LOG = 
      LogFactory.getLog(DistBlockFixerMapper.class);
    
    /**
     * splits the input files into tasks handled by a single node
     * we have to read the input files to do this based on a number of 
     * items in a sequence
     */
    @Override
    public List <InputSplit> getSplits(JobContext job) 
      throws IOException {
      long filesPerTask = DistBlockFixer.filesPerTask(job.getConfiguration());

      Path[] inPaths = getInputPaths(job);

      List<InputSplit> splits = new ArrayList<InputSplit>();

      long fileCounter = 0;

      for (Path inPath: inPaths) {
        
        FileSystem fs = inPath.getFileSystem(job.getConfiguration());      

        if (!fs.getFileStatus(inPath).isDir()) {
          throw new IOException(inPath.toString() + " is not a directory");
        }

        FileStatus[] inFiles = fs.listStatus(inPath);

        for (FileStatus inFileStatus: inFiles) {
          Path inFile = inFileStatus.getPath();
          
          if (!inFileStatus.isDir() &&
              (inFile.getName().equals(job.getJobName() + IN_FILE_SUFFIX))) {

            fileCounter++;
            SequenceFile.Reader inFileReader = 
              new SequenceFile.Reader(fs, inFile, job.getConfiguration());
            
            long startPos = inFileReader.getPosition();
            long counter = 0;
            
            // create an input split every filesPerTask items in the sequence
            LongWritable key = new LongWritable();
            Text value = new Text();
            try {
              while (inFileReader.next(key, value)) {
                if (counter % filesPerTask == filesPerTask - 1L) {
                  splits.add(new FileSplit(inFile, startPos, 
                                           inFileReader.getPosition() - 
                                           startPos,
                                           null));
                  startPos = inFileReader.getPosition();
                }
                counter++;
              }
              
              // create input split for remaining items if necessary
              // this includes the case where no splits were created by the loop
              if (startPos != inFileReader.getPosition()) {
                splits.add(new FileSplit(inFile, startPos,
                                         inFileReader.getPosition() - startPos,
                                         null));
              }
            } finally {
              inFileReader.close();
            }
          }
        }
      }

      LOG.info("created " + splits.size() + " input splits from " +
               fileCounter + " files");
      
      return splits;
    }

    /**
     * indicates that input file can be split
     */
    @Override
    public boolean isSplitable (JobContext job, Path file) {
      return true;
    }
  }


  /**
   * mapper for fixing stripes with corrupt blocks
   */
  static class DistBlockFixerMapper
    extends Mapper<LongWritable, Text, Text, Text> {

    protected static final Log LOG = 
      LogFactory.getLog(DistBlockFixerMapper.class);

    /**
     * fix a stripe
     */
    @Override
    public void map(LongWritable key, Text fileText, Context context) 
      throws IOException, InterruptedException {
      
      BlockFixerHelper helper = 
        new BlockFixerHelper(context.getConfiguration());

      String fileStr = fileText.toString();
      LOG.info("fixing " + fileStr);

      Path file = new Path(fileStr);

      try {
        boolean fixed = helper.fixFile(file, context);
        
        if (fixed) {
          context.getCounter(Counter.FILES_SUCCEEDED).increment(1L);
        } else {
          context.getCounter(Counter.FILES_NOACTION).increment(1L);
        }
      } catch (Exception e) {
        LOG.error(StringUtils.stringifyException(e));

        // report file as failed
        context.getCounter(Counter.FILES_FAILED).increment(1L);
        String outkey = fileStr;
        String outval = "failed";
        context.write(new Text(outkey), new Text(outval));
      }
      
      context.progress();
    }
  }

  /**
   * Update {@link lastStatus} so that it can be viewed from outside
   */
  private void updateStatus() {
    int highPriorityFiles = 0;
    int lowPriorityFiles = 0;
    List<JobStatus> jobs = new ArrayList<JobStatus>();
    List<String> highPriorityFileNames = new ArrayList<String>();
    for (Map.Entry<String, CorruptFileInfo> e : fileIndex.entrySet()) {
      String fileName = e.getKey();
      CorruptFileInfo fileInfo = e.getValue();
      if (fileInfo.getHighestPriority() > 0) {
        highPriorityFileNames.add(fileName);
        highPriorityFiles += 1;
      } else {
        lowPriorityFiles += 1;
      }
    }
    for (Job job : jobIndex.keySet()) {
      String url = job.getTrackingURL();
      String name = job.getJobName();
      JobID jobId = job.getID();
      jobs.add(new BlockFixer.JobStatus(jobId, name, url));
    }
    lastStatus = new BlockFixer.Status(highPriorityFiles, lowPriorityFiles,
        jobs, highPriorityFileNames);
    RaidNodeMetrics.getInstance().corruptFilesHighPri.set(highPriorityFiles);
    RaidNodeMetrics.getInstance().corruptFilesLowPri.set(lowPriorityFiles);
    LOG.info("Update status done." + lastStatus.toString());
  }

  @Override
  public BlockFixer.Status getStatus() {
    return lastStatus;
  }

}
