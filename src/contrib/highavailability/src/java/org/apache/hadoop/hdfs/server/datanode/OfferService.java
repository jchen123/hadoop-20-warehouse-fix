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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Date;
import java.util.Iterator;
import java.util.Collection;
import java.util.LinkedList;
import java.util.TreeSet;
import java.util.Random;
import java.text.SimpleDateFormat;

import org.apache.hadoop.ipc.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.hdfs.AvatarZooKeeperClient;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.UnregisteredDatanodeException;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.BlockReport;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DisallowedDatanodeException;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.protocol.AvatarProtocol;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

public class OfferService implements Runnable {

  public static final Log LOG = LogFactory.getLog(OfferService.class.getName());
  private int numFrequentReports;
  private int freqReportsCoeff;
  
  private final static int BACKOFF_DELAY = 10 * 60 * 1000;

  long lastHeartbeat = 0;
  volatile boolean shouldRun = true;
  long lastBlockReport = 0;
  boolean resetBlockReportTime = true;
  long blockReceivedRetryInterval;
  int reportsSinceRegister;
  AvatarDataNode anode;
  DatanodeProtocol namenode;
  AvatarProtocol avatarnode;
  InetSocketAddress namenodeAddress;
  InetSocketAddress avatarnodeAddress;
  DatanodeRegistration dnRegistration = null;
  FSDatasetInterface data;
  AvatarZooKeeperClient zkClient = null;
  DataNodeMetrics myMetrics;
  private static final Random R = new Random();
  private int backlogSize; // if we accumulate this many blockReceived, then it is time
                           // to send a block report. Otherwise the receivedBlockList
                           // might exceed our Heap size.
  /**
   * A data structure to store Block and delHints together
   */
  private static class BlockInfo extends Block {
    String delHints;

    BlockInfo(Block blk, String delHints) {
      super(blk);
      this.delHints = delHints;
    }
  }
  private TreeSet<BlockInfo> retryBlockList = new TreeSet<BlockInfo>();
  private TreeSet<BlockInfo> receivedBlockList = new TreeSet<BlockInfo>();
  private long lastBlockReceivedFailed = 0; 

  /**
   * Offer service to the specified namenode
   */
  public OfferService(AvatarDataNode anode, 
                      DatanodeProtocol namenode, InetSocketAddress namenodeAddress,
                      AvatarProtocol avatarnode, InetSocketAddress avatarnodeAddress) {
    numFrequentReports = anode.getConf().getInt(
        "ha.blockreport.frequentReports", 4);
    freqReportsCoeff = anode.getConf()
    .getInt("ha.blockreport.frequent.coeff", 2);
    this.anode = anode;
    this.namenode = namenode;
    this.avatarnode = avatarnode;
    this.namenodeAddress = namenodeAddress;
    this.avatarnodeAddress = avatarnodeAddress;
    zkClient = new AvatarZooKeeperClient(anode.getConf(), null);
    
    reportsSinceRegister = numFrequentReports;
    
    dnRegistration = anode.dnRegistration;
    data = anode.data;
    myMetrics = anode.myMetrics;
    scheduleBlockReport(anode.initialBlockReportDelay);
    backlogSize = anode.getConf().getInt("dfs.datanode.blockreceived.backlog", 10000);
    blockReceivedRetryInterval = anode.getConf().getInt(
        "dfs.datanode.blockreceived.retry.internval", 10000);
  }

  public void stop() {
    shouldRun = false;
  }
  
  private boolean isPrimaryService() {
    try {
      InetSocketAddress addr = DataNode
          .getNameNodeAddress(this.anode.getConf());
      String addrStr = addr.getHostName() + ":" + addr.getPort();
      Stat stat = new Stat();
      String actual = zkClient.getPrimaryAvatarAddress(addrStr, stat, true);
      String offerServiceAddress = this.namenodeAddress.getHostName() + ":"
          + this.namenodeAddress.getPort();
      return actual.equalsIgnoreCase(offerServiceAddress);
    } catch (Exception ex) {
      LOG.error("Could not get the primary from ZooKeeper", ex);
    }
    return false;
  }

  public void run() {
    while (shouldRun) {
      try {
        if (isPrimaryService()) {
          this.anode.setPrimaryOfferService(this);
        }
        offerService();
      } catch (Exception e) {
        LOG.error("OfferService encountered exception " +
                   StringUtils.stringifyException(e));
      }
    }
  }

  public void offerService() throws Exception {
     
    LOG.info("using BLOCKREPORT_INTERVAL of " + anode.blockReportInterval + "msec" + 
       " Initial delay: " + anode.initialBlockReportDelay + "msec");

    //
    // Now loop for a long time....
    //
    while (shouldRun) {
      try {

        // If we are falling behind in confirming blockReceived to NN, then
        // we clear the backlog and schedule a block report. This scenario
        // is likely to arise if one of the NN is down for an extended period.
        if (receivedBlockList.size() + retryBlockList.size() > backlogSize) {
          LOG.warn("The backlog of blocks to be confirmed has exceeded the " +
                   " configured maximum of " + backlogSize +
                   " records. Cleaning up and scheduling a block report.");
          synchronized(receivedBlockList) {
            receivedBlockList.clear();
            retryBlockList.clear();
          }
          scheduleBlockReport(0);
        }

        long startTime = anode.now();

        //
        // Every so often, send heartbeat or block-report
        //
        if (startTime - lastHeartbeat > anode.heartBeatInterval) {
          //
          // All heartbeat messages include following info:
          // -- Datanode name
          // -- data transfer port
          // -- Total capacity
          // -- Bytes remaining
          //
          lastHeartbeat = startTime;
          DatanodeCommand[] cmds = namenode.sendHeartbeat(dnRegistration,
                                                       data.getCapacity(),
                                                       data.getDfsUsed(),
                                                       data.getRemaining(),
                                                       anode.xmitsInProgress.get(),
                                                       anode.getXceiverCount());
          myMetrics.heartbeats.inc(anode.now() - startTime);
          //LOG.info("Just sent heartbeat, with name " + localName);
          if (!processCommand(cmds))
            continue;
        }
            
        // check if there are newly received blocks
        BlockInfo [] blockArray=null;
        int numBlocks = 0;
        synchronized(receivedBlockList) {
            // retry previously failed blocks every few seconds
          if (lastBlockReceivedFailed + blockReceivedRetryInterval 
                < anode.now()) {
            for (BlockInfo blk : retryBlockList) {
              receivedBlockList.add(blk);
            }
            retryBlockList.clear();
          }
          numBlocks = receivedBlockList.size();
          if (numBlocks > 0) {
            blockArray = receivedBlockList.toArray(new BlockInfo[numBlocks]);
          }
        }
        if (blockArray != null) {

          String[] delHintArray = new String[numBlocks];
          Block[]  blist = new Block[numBlocks];
          for (int i = 0; i < numBlocks; i++) {
            delHintArray[i] = blockArray[i].delHints;
            blist[i] = new Block(blockArray[i]);
          }

          Block[] failed = avatarnode.blockReceivedNew(dnRegistration, blist, 
                                                       delHintArray);
          synchronized (receivedBlockList) {
            // Blocks that do not belong to an Inode are saved for retransmisions
            for (int i = 0; i < failed.length; i++) {
              BlockInfo info = null;
              for(int j = 0; j < blockArray.length; j++) {
                if (blockArray[j].equals(failed[i])) {
                  info = blockArray[j];
                  break;
                }
              }
              if (info == null) {
                LOG.warn("BlockReceived failed for block " + failed[i] +
                         " but it is not in our request list.");
              } else if (receivedBlockList.contains(info)) {
                 // Insert into retry list only if the block was not deleted
                 // on this datanode. That is why we have to recheck if the
                 // block still exists in receivedBlockList.
                 LOG.info("Block " + info + " does not belong to any file " +
                          "on namenode " + avatarnodeAddress + " Retry later.");
                 retryBlockList.add(info);
                 lastBlockReceivedFailed = anode.now();
              } else {
                 LOG.info("Block " + info + " does not belong to any file " +
                          "on namenode " + avatarnodeAddress + 
                          " but will not be retried.");
              }
            }
            for (int i = 0; i < blockArray.length; i++) {
              receivedBlockList.remove(blockArray[i]);
            }
          }
        }

        // send block report
        if (startTime - lastBlockReport > anode.blockReportInterval) {
          //
          // Send latest blockinfo report if timer has expired.
          // Get back a list of local block(s) that are obsolete
          // and can be safely GC'ed.
          //
          long brStartTime = anode.now();
          Block[] bReport = data.getBlockReport();
          DatanodeCommand cmd = avatarnode.blockReportNew(dnRegistration,
                  new BlockReport(BlockListAsLongs.convertToArrayLongs(bReport)));
          if (cmd != null &&
              cmd.getAction() == DatanodeProtocols.DNA_BACKOFF) {
            // The Standby is catching up and we need to reschedule
            scheduleBlockReport(BACKOFF_DELAY);
            continue;
          }
          long brTime = anode.now() - brStartTime;
          myMetrics.blockReports.inc(brTime);
          LOG.info("BlockReport of " + bReport.length +
              " blocks got processed in " + brTime + " msecs on " +
              namenodeAddress);
          if (reportsSinceRegister < numFrequentReports) {
            // Schedule block reports more frequently for the first 
            // numFrequentReports reports. This helps the standby node
            // get full information about block locations faster
            int frequentReportsInterval = 
              (int) (anode.blockReportInterval / freqReportsCoeff);
            scheduleBlockReport(frequentReportsInterval);
            reportsSinceRegister++;
            LOG.info("Scheduling frequent report #" + reportsSinceRegister);
          } else if (resetBlockReportTime) {
            //
            // If we have sent the first block report, then wait a random
            // time before we start the periodic block reports.
            //
            lastBlockReport = startTime - R.nextInt((int)(anode.blockReportInterval));
            resetBlockReportTime = false;
          } else {
          
            /* say the last block report was at 8:20:14. The current report 
             * should have started around 9:20:14 (default 1 hour interval). 
             * If current time is :
             *   1) normal like 9:20:18, next report should be at 10:20:14
             *   2) unexpected like 11:35:43, next report should be at 12:20:14
             */
            lastBlockReport += (anode.now() - lastBlockReport) / 
                               anode.blockReportInterval * anode.blockReportInterval;
          }
          processCommand(cmd);
        }

        // start block scanner is moved to the Dataode.run()
            
        //
        // There is no work to do;  sleep until hearbeat timer elapses, 
        // or work arrives, and then iterate again.
        //
        long waitTime = anode.heartBeatInterval - (System.currentTimeMillis() - lastHeartbeat);
        synchronized(receivedBlockList) {
          if (waitTime > 0 && receivedBlockList.size() == 0 && shouldRun) {
            try {
              receivedBlockList.wait(waitTime);
            } catch (InterruptedException ie) {
            }
          }
        } // synchronized
      } catch(RemoteException re) {
        // If either the primary or standby NN throws these exceptions, this
        // datanode will exit. I think this is the right behaviour because
        // the excludes list on both namenode better be the same.
        String reClass = re.getClassName(); 
        if (UnregisteredDatanodeException.class.getName().equals(reClass) ||
            DisallowedDatanodeException.class.getName().equals(reClass) ||
            IncorrectVersionException.class.getName().equals(reClass)) {
          LOG.warn("DataNode is shutting down: " + 
                   StringUtils.stringifyException(re));
          anode.shutdownDN();
          return;
        }
        LOG.warn(StringUtils.stringifyException(re));
      } catch (IOException e) {
        LOG.warn(StringUtils.stringifyException(e));
      }
    } // while (shouldRun)
  } // offerService

  /**
   * Process an array of datanode commands
   * 
   * @param cmds an array of datanode commands
   * @return true if further processing may be required or false otherwise. 
   */
  private boolean processCommand(DatanodeCommand[] cmds) {
    if (cmds != null) {
      boolean isPrimary = this.anode.isPrimaryOfferService(this);
      for (DatanodeCommand cmd : cmds) {
        try {
          if (cmd.getAction() != DatanodeProtocol.DNA_REGISTER && !isPrimary) {
            if (isPrimaryService()) {
              // The failover has occured. Need to update the datanode knowledge
              this.anode.setPrimaryOfferService(this);
            } else {
              continue;
            }
          } else if (cmd.getAction() == DatanodeProtocol.DNA_REGISTER && 
                        !isPrimaryService()) {
            // Standby issued a DNA_REGISTER. Enter the mode of sending frequent
            // heartbeats
            LOG.info("Registering with Standby. Start frequent block reports");
            reportsSinceRegister = 0;
            if (isPrimary) {
              // This information is out of date
              this.anode.setPrimaryOfferService(null);
            }
          }
          if (processCommand(cmd) == false) {
            return false;
          }
        } catch (IOException ioe) {
          LOG.warn("Error processing datanode Command", ioe);
        }
      }
    }
    return true;
  }
  
    /**
     * 
     * @param cmd
     * @return true if further processing may be required or false otherwise. 
     * @throws IOException
     */
  private boolean processCommand(DatanodeCommand cmd) throws IOException {
    if (cmd == null)
      return true;
    final BlockCommand bcmd = cmd instanceof BlockCommand? (BlockCommand)cmd: null;

    switch(cmd.getAction()) {
    case DatanodeProtocol.DNA_TRANSFER:
      // Send a copy of a block to another datanode
      anode.transferBlocks(bcmd.getBlocks(), bcmd.getTargets());
      myMetrics.blocksReplicated.inc(bcmd.getBlocks().length);
      break;
    case DatanodeProtocol.DNA_INVALIDATE:
      //
      // Some local block(s) are obsolete and can be 
      // safely garbage-collected.
      //
      Block toDelete[] = bcmd.getBlocks();
      try {
        if (anode.blockScanner != null) {
          anode.blockScanner.deleteBlocks(toDelete);
        }
        anode.removeReceivedBlocks(toDelete);
        data.invalidate(toDelete);
      } catch(IOException e) {
        anode.checkDiskError();
        throw e;
      }
      myMetrics.blocksRemoved.inc(toDelete.length);
      break;
    case DatanodeProtocol.DNA_SHUTDOWN:
      // shut down the data node
      anode.shutdownDN();
      return false;
    case DatanodeProtocol.DNA_REGISTER:
      // namenode requested a registration - at start or if NN lost contact
      LOG.info("AvatarDatanodeCommand action: DNA_REGISTER");
      if (shouldRun) {
        anode.register(namenode, namenodeAddress);
        scheduleBlockReport(0);
      }
      break;
    case DatanodeProtocol.DNA_FINALIZE:
      anode.getStorage().finalizeUpgrade();
      break;
    case UpgradeCommand.UC_ACTION_START_UPGRADE:
      // start distributed upgrade here
      anode.upgradeManager.processUpgradeCommand((UpgradeCommand)cmd);
      break;
    case DatanodeProtocol.DNA_RECOVERBLOCK:
      anode.recoverBlocks(bcmd.getBlocks(), bcmd.getTargets());
      break;
    default:
      LOG.warn("Unknown DatanodeCommand action: " + cmd.getAction());
    }
    return true;
  }

  /**
   * This methods  arranges for the data node to send the block report at the next heartbeat.
   */
  public void scheduleBlockReport(long delay) {
    if (delay > 0) { // send BR after random delay
      lastBlockReport = System.currentTimeMillis()
                            - ( anode.blockReportInterval - R.nextInt((int)(delay)));
    } else { // send at next heartbeat
      lastBlockReport = lastHeartbeat - anode.blockReportInterval;
    }
    resetBlockReportTime = true; // reset future BRs for randomness
  }

  /**
   * Inform the namenode that we have received a block
   */
  void notifyNamenodeReceivedBlock(Block block, String delHint) {
    if (block==null || delHint==null) {
      throw new IllegalArgumentException(block==null?"Block is null":"delHint is null");
    }
    synchronized (receivedBlockList) {
      receivedBlockList.add(new BlockInfo(block, delHint));
      receivedBlockList.notifyAll();
    }
  }

  /**
   * Remove  blocks from blockReceived queues
   */
  void removeReceivedBlocks(Block[] removeList) {
    synchronized(receivedBlockList) {
      for (int i = 0; i < removeList.length; i++) {
        if (receivedBlockList.remove(removeList[i])) {
          LOG.info("Block deletion command deleted from receivedBlockList " + 
                   removeList[i]);
        }
        else if (retryBlockList.remove(removeList[i])) {
          LOG.info("Block deletion command deleted from retryBlockList " + 
                   removeList[i]);
        } else {
          LOG.info("Block deletion command did not find block in " +
                   "pending blockReceived. " + removeList[i]);
        }
      }
    }
  }
}


