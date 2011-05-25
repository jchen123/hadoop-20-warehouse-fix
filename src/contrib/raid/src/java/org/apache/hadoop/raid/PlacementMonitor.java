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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.util.StringUtils;

/**
 * Monitors and potentially fixes placement of blocks in RAIDed files.
 */
public class PlacementMonitor {
  public static final Log LOG = LogFactory.getLog(PlacementMonitor.class);

  Configuration conf;
  Map<ErasureCodeType, Map<Integer, Long>> blockHistograms;
  private volatile Map<ErasureCodeType, Map<Integer, Long>> lastBlockHistograms;
  private volatile long lastUpdateStartTime = 0L;
  private volatile long lastUpdateFinishTime = 0L;
  private volatile long lastUpdateUsedTime = 0L;
  RaidNodeMetrics metrics;
  BlockMover blockMover;

  final static String NUM_MOVING_THREADS_KEY = "hdfs.raid.block.move.threads";
  final static String SIMULATE_KEY = "hdfs.raid.block.move.simulate";
  final static String BLOCK_MOVE_QUEUE_LENGTH_KEY = "hdfs.raid.block.move.queue.length";
  final static int DEFAULT_NUM_MOVING_THREADS = 10;
  final static int DEFAULT_BLOCK_MOVE_QUEUE_LENGTH = 30000;
  final int rsCodePriorityOffset;

  PlacementMonitor(Configuration conf) throws IOException {
    this.conf = conf;
    this.blockHistograms = createEmptyHistograms();
    int numMovingThreads = conf.getInt(
        NUM_MOVING_THREADS_KEY, DEFAULT_NUM_MOVING_THREADS);
    int maxMovingQueueSize = conf.getInt(
        BLOCK_MOVE_QUEUE_LENGTH_KEY, DEFAULT_BLOCK_MOVE_QUEUE_LENGTH);
    boolean simulate = conf.getBoolean(SIMULATE_KEY, true);
    this.rsCodePriorityOffset = RaidNode.getStripeLength(conf) - 1;
    // For RS file with 3 blocks on same node or more and
    // XOR file with (stripeLength + 2) on same node, always submit the move
    int alwaysSubmitPriorityLevel = rsCodePriorityOffset + 3;
    blockMover = new BlockMover(
        numMovingThreads, maxMovingQueueSize, simulate,
        alwaysSubmitPriorityLevel, conf);
    this.metrics = RaidNodeMetrics.getInstance();
  }

  private Map<ErasureCodeType, Map<Integer, Long>> createEmptyHistograms() {
    Map<ErasureCodeType, Map<Integer, Long>> histo =
        new HashMap<ErasureCodeType, Map<Integer, Long>>();
    for (ErasureCodeType type : ErasureCodeType.values()) {
      histo.put(type, new HashMap<Integer, Long>());
    }
    return new EnumMap<ErasureCodeType, Map<Integer, Long>>(histo);
  }

  public void start() {
    blockMover.start();
  }

  public void stop() {
    blockMover.stop();
  }

  public void startCheckingFiles() {
    lastUpdateStartTime = RaidNode.now();
  }

  public int getMovingQueueSize() {
    return blockMover.getQueueSize();
  }

  public void checkFile(FileSystem srcFs, FileStatus srcFile,
            FileSystem parityFs, Path partFile, HarIndex.IndexEntry entry,
            ErasureCodeType code) throws IOException {
    if (srcFs.getUri().equals(parityFs.getUri())) {
      checkLocatedBlocks(
          getLocatedBlocks(srcFs, srcFile),
          getLocatedBlocks(parityFs, partFile, entry.startOffset, entry.length),
          code);
    } else { 
      // TODO: Move blocks in two clusters separately
      LOG.warn("Source and parity are in different file system. " +
          " source:" + srcFs.getUri() + " parity:" + parityFs.getUri() +
          ". Skip.");
    }
  }

  public void checkFile(FileSystem srcFs, FileStatus srcFile,
                        FileSystem parityFs, FileStatus parityFile,
                        ErasureCodeType code)
      throws IOException {
    if (srcFs.equals(parityFs)) {
      checkLocatedBlocks(
          getLocatedBlocks(srcFs, srcFile),
          getLocatedBlocks(parityFs, parityFile), code);
    } else {
      // TODO: Move blocks in two clusters separately
      LOG.warn("Source and parity are in different file systems. Skip");
    }
  }

  private LocatedBlocks getLocatedBlocks(
    FileSystem fs, FileStatus stat) throws IOException {
    return getLocatedBlocks(
      fs, stat.getPath(), 0, stat.getLen());
  }

  private LocatedBlocks getLocatedBlocks(
    FileSystem fs, Path path, long start, long length)
      throws IOException {
    if (!(fs instanceof DistributedFileSystem)) { return null; }
    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    return dfs.getClient().namenode.getBlockLocations(
      path.toUri().getPath(), start, length);
  }

  void checkLocatedBlocks(LocatedBlocks srcBlocks,
      LocatedBlocks parityBlocks, ErasureCodeType code) throws IOException {
    if (srcBlocks == null || parityBlocks == null) {
      return;
    }
    int stripeLength = RaidNode.getStripeLength(conf);
    int parityLength = code == ErasureCodeType.XOR ?
        1 : RaidNode.rsParityLength(conf);
    int numStripes = (int)Math.ceil(
        (float)(srcBlocks.getLocatedBlocks().size()) / stripeLength);

    Map<DatanodeInfo, Integer> nodeToNumBlocks =
        new HashMap<DatanodeInfo, Integer>();
    Set<DatanodeInfo> nodesInThisStripe = new HashSet<DatanodeInfo>();

    for (int stripeIndex = 0; stripeIndex < numStripes; ++stripeIndex) {

      List<LocatedBlock> stripeBlocks = getStripeBlocks(
          stripeIndex, srcBlocks, stripeLength, parityBlocks, parityLength);

      countBlocksOnEachNode(stripeBlocks, nodeToNumBlocks, nodesInThisStripe);

      updateBlockPlacementHistogram(nodeToNumBlocks, blockHistograms.get(code));

      submitBlockMoves(nodeToNumBlocks, stripeBlocks, nodesInThisStripe, code);

    }
  }

  private static List<LocatedBlock> getStripeBlocks(int stripeIndex,
      LocatedBlocks srcBlocks, int stripeLength,
      LocatedBlocks parityBlocks, int parityLength) {
    List<LocatedBlock> stripeBlocks = new LinkedList<LocatedBlock>();
    // Adding source blocks
    int stripeStart = stripeLength * stripeIndex;
    int stripeEnd = Math.min(
        stripeStart + stripeLength, srcBlocks.getLocatedBlocks().size());
    if (stripeStart < stripeEnd) {
      stripeBlocks.addAll(
          srcBlocks.getLocatedBlocks().subList(stripeStart, stripeEnd));
    }
    // Adding parity blocks
    stripeStart = parityLength * stripeIndex;
    stripeEnd = Math.min(
        stripeStart + parityLength, parityBlocks.getLocatedBlocks().size());
    if (stripeStart < stripeEnd) {
      stripeBlocks.addAll(
          parityBlocks.getLocatedBlocks().subList(stripeStart, stripeEnd));
    }
    return stripeBlocks;
  }

  private static void countBlocksOnEachNode(List<LocatedBlock> stripeBlocks,
      Map<DatanodeInfo, Integer> nodeToNumBlocks,
      Set<DatanodeInfo> nodesInThisStripe) {
    nodeToNumBlocks.clear();
    nodesInThisStripe.clear();
    for (LocatedBlock block : stripeBlocks) {
      for (DatanodeInfo node : block.getLocations()) {
        Integer n = nodeToNumBlocks.get(node);
        if (n == null) {
          n = 0;
        }
        nodeToNumBlocks.put(node, n + 1);
        nodesInThisStripe.add(node);
      }
    }
  }

  private static void updateBlockPlacementHistogram(
      Map<DatanodeInfo, Integer> nodeToNumBlocks,
      Map<Integer, Long> blockHistogram) {
    for (Integer numBlocks : nodeToNumBlocks.values()) {
      Long n = blockHistogram.get(numBlocks - 1);
      if (n == null) {
        n = 0L;
      }
      // Number of neighbor blocks to number of blocks
      blockHistogram.put(numBlocks - 1, n + 1);
    }
  }

  private void submitBlockMoves(Map<DatanodeInfo, Integer> nodeToNumBlocks,
      List<LocatedBlock> stripeBlocks, Set<DatanodeInfo> excludedNodes,
      ErasureCodeType code) {
    // For all the nodes that has more than 2 blocks, find and move the blocks
    // so that there are only one block left on this node.
    for (DatanodeInfo node : nodeToNumBlocks.keySet()) {
      int colocatedBlocks = nodeToNumBlocks.get(node);
      if (colocatedBlocks <= 1) {
        continue;
      }
      boolean skip = true;
      for (LocatedBlock block : stripeBlocks) {
        for (DatanodeInfo otherNode : block.getLocations()) {
          if (node.equals(otherNode)) {
            if (skip) {
              // leave the first block where it is
              skip = false;
              break;
            }
            int priority = calculatePriority(colocatedBlocks, code);
            blockMover.move(block, node, excludedNodes, priority);
            break;
          }
        }
      }
    }
  }

  private int calculatePriority(int colocatedBlocks, ErasureCodeType code) {
    int priority = colocatedBlocks;
    if (code == ErasureCodeType.RS) {
      priority += rsCodePriorityOffset;
    }
    return priority;
  }

  /**
   * Report the placement histogram to {@link RaidNodeMetrics}. This should only
   * be called right after a complete parity file traversal is done.
   */
  public void clearAndReport() {
    synchronized (metrics) {
      int extra = 0;
      for (Entry<Integer, Long> e :
          blockHistograms.get(ErasureCodeType.RS).entrySet()) {
        if (e.getKey() < metrics.misplacedRs.length - 1) {
          metrics.misplacedRs[e.getKey()].set(e.getValue());
        } else {
          extra += e.getValue();
        }
      }
      metrics.misplacedRs[metrics.misplacedRs.length - 1].set(extra);
      extra = 0;
      for (Entry<Integer, Long> e :
          blockHistograms.get(ErasureCodeType.XOR).entrySet()) {
        if (e.getKey() < metrics.misplacedXor.length - 1) {
          metrics.misplacedXor[e.getKey()].set(e.getValue());
        } else {
          extra += e.getValue();
        }
      }
      metrics.misplacedXor[metrics.misplacedXor.length - 1].set(extra);
    }
    lastBlockHistograms = blockHistograms;
    lastUpdateFinishTime = RaidNode.now();
    lastUpdateUsedTime = lastUpdateFinishTime - lastUpdateStartTime;
    LOG.info("Reporting metrices:\n" + toString());
    blockHistograms = createEmptyHistograms();
  }

  @Override
  public String toString() {
    if (lastBlockHistograms == null) {
      return "Not available";
    }
    String result = "";
    for (ErasureCodeType code : ErasureCodeType.values()) {
      Map<Integer, Long> histo = lastBlockHistograms.get(code);
      result += code + " Blocks\n";
      List<Integer> neighbors = new ArrayList<Integer>();
      neighbors.addAll(histo.keySet());
      Collections.sort(neighbors);
      for (Integer i : neighbors) {
        Long numBlocks = histo.get(i);
        result += i + " co-localted blocks:" + numBlocks + "\n";
      }
    }
    return result;
  }

  public String htmlTable() {
    if (lastBlockHistograms == null) {
      return "Not available";
    }
    int max = computeMaxColocatedBlocks();
    String head = "";
    for (int i = 0; i <= max; ++i) {
      head += JspUtils.td(i + "");
    }
    head = JspUtils.tr(JspUtils.td("CODE") + head);
    String result = head;
    for (ErasureCodeType code : ErasureCodeType.values()) {
      String row = JspUtils.td(code.toString());
      Map<Integer, Long> histo = lastBlockHistograms.get(code);
      for (int i = 0; i <= max; ++i) {
        Long numBlocks = histo.get(i);
        numBlocks = numBlocks == null ? 0 : numBlocks;
        row += JspUtils.td(StringUtils.humanReadableInt(numBlocks));
      }
      row = JspUtils.tr(row);
      result += row;
    }
    return JspUtils.table(result);
  }

  public long lastUpdateTime() {
    return lastUpdateFinishTime;
  }

  public long lastUpdateUsedTime() {
    return lastUpdateUsedTime;
  }

  private int computeMaxColocatedBlocks() {
    int max = 0;
    for (ErasureCodeType code : ErasureCodeType.values()) {
      Map<Integer, Long> histo = lastBlockHistograms.get(code);
      for (Integer i : histo.keySet()) {
        max = Math.max(i, max);
      }
    }
    return max;
  }
}
