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

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.BlockSender;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;


/**
 * contains the core functionality of the block fixer
 *
 * configuration options:
 * raid.blockfix.classname         - the class name of the block fixer 
 *                                   implementation to use
 *
 * raid.blockfix.interval          - interval between checks for corrupt files
 *
 * raid.blockfix.read.timeout      - read time out
 *
 * raid.blockfix.write.timeout     - write time out
 */
public abstract class BlockFixer extends Configured implements Runnable {

  public static final String BLOCKFIX_CLASSNAME = "raid.blockfix.classname";
  public static final String BLOCKFIX_INTERVAL = "raid.blockfix.interval";
  public static final String BLOCKFIX_READ_TIMEOUT = 
    "raid.blockfix.read.timeout";
  public static final String BLOCKFIX_WRITE_TIMEOUT = 
    "raid.blockfix.write.timeout";

  public static final long DEFAULT_BLOCKFIX_INTERVAL = 60 * 1000; // 1 min

  public static BlockFixer createBlockFixer(Configuration conf)
    throws ClassNotFoundException {
    try {
      // default to distributed block fixer
      Class<?> blockFixerClass =
        conf.getClass(BLOCKFIX_CLASSNAME, DistBlockFixer.class);
      if (!BlockFixer.class.isAssignableFrom(blockFixerClass)) {
        throw new ClassNotFoundException("not an implementation of blockfixer");
      }
      Constructor<?> constructor =
        blockFixerClass.getConstructor(new Class[] {Configuration.class} );
      return (BlockFixer) constructor.newInstance(conf);
    } catch (NoSuchMethodException e) {
      throw new ClassNotFoundException("cannot construct blockfixer", e);
    } catch (InstantiationException e) {
      throw new ClassNotFoundException("cannot construct blockfixer", e);
    } catch (IllegalAccessException e) {
      throw new ClassNotFoundException("cannot construct blockfixer", e);
    } catch (InvocationTargetException e) {
      throw new ClassNotFoundException("cannot construct blockfixer", e);
    }
  }

  private long numFilesFixed = 0;
  private long numFileFixFailures = 0;

  public volatile boolean running = true;

  // interval between checks for corrupt files
  protected long blockFixInterval;

  public BlockFixer(Configuration conf) {
    super(conf);
    blockFixInterval =
      getConf().getLong(BLOCKFIX_INTERVAL, DEFAULT_BLOCKFIX_INTERVAL);
  }

  @Override
  public abstract void run();

  /**
   * Returns the number of file fix failures.
   */
  public synchronized long fileFixFailures() {
    return numFileFixFailures;
  }

  /**
   * increments the number of failures that have been encountered.
   */
  protected synchronized void incrFileFixFailures() {
    RaidNodeMetrics.getInstance().fileFixFailures.inc();
    numFileFixFailures++;
  }

  /**
   * increments the number of failures that have been encountered.
   */
  protected synchronized void incrFileFixFailures(long incr) {
    RaidNodeMetrics.getInstance().fileFixFailures.inc(incr);
    numFileFixFailures += incr;
  }
  /**
   * returns the number of files that have been fixed by this block fixer
   */
  public synchronized long filesFixed() {
    return numFilesFixed;
  }

  /**
   * increments the number of files that have been fixed by this block fixer
   */
  protected synchronized void incrFilesFixed() {
    RaidNodeMetrics.getInstance().filesFixed.inc();
    numFilesFixed++;
  }

  /**
   * increments the number of files that have been fixed by this block fixer
   */
  protected synchronized void incrFilesFixed(long incr) {
    if (incr < 0) {
      throw new IllegalArgumentException("cannot increment by negative value " +
                                         incr);
    }
    
    RaidNodeMetrics.getInstance().filesFixed.inc(incr);
    numFilesFixed += incr;
  }

  static boolean isSourceFile(String p, String[] destPrefixes) {
    for (String destPrefix: destPrefixes) {
      if (p.startsWith(destPrefix)) {
        return false;
      }
    }
    return true;
  }

  String[] destPrefixes() throws IOException {
    String xorPrefix = RaidNode.xorDestinationPath(getConf()).toUri().getPath();
    if (!xorPrefix.endsWith(Path.SEPARATOR)) {
      xorPrefix += Path.SEPARATOR;
    }
    String rsPrefix = RaidNode.rsDestinationPath(getConf()).toUri().getPath();
    if (!rsPrefix.endsWith(Path.SEPARATOR)) {
      rsPrefix += Path.SEPARATOR;
    }
    return new String[]{xorPrefix, rsPrefix};
  }

  static boolean doesParityDirExist(
      FileSystem parityFs, String path, String[] destPrefixes)
      throws IOException {
    // Check if it is impossible to have a parity file. We check if the
    // parent directory of the corrupt file exists under a parity path.
    // If the directory does not exist, the parity file cannot exist.
    String parentUriPath = new Path(path).getParent().toUri().getPath();
    // Remove leading '/', if any.
    if (parentUriPath.startsWith(Path.SEPARATOR)) {
      parentUriPath = parentUriPath.substring(1);
    }
    boolean parityCanExist = false;
    for (String destPrefix: destPrefixes) {
      Path parityDir = new Path(destPrefix, parentUriPath);
      if (parityFs.exists(parityDir)) {
        parityCanExist = true;
        break;
      }
    }
    return parityCanExist;
  }

  void filterUnfixableSourceFiles(FileSystem parityFs, Iterator<String> it)
      throws IOException {
    String[] destPrefixes = destPrefixes();
    while (it.hasNext()) {
      String p = it.next();
      if (isSourceFile(p, destPrefixes) &&
          !doesParityDirExist(parityFs, p, destPrefixes)) {
          it.remove();
      }
    }
  }

  /**
   * this class implements the actual fixing functionality
   * we keep this in a separate class so that 
   * the distributed block fixer can use it
   */ 
  static class BlockFixerHelper extends Configured {

    public static final Log LOG = LogFactory.getLog(BlockFixer.
                                                    BlockFixerHelper.class);

    private String xorPrefix;
    private String rsPrefix;
    private XOREncoder1 xorEncoder;
    private XORDecoder xorDecoder;
    private ReedSolomonEncoder rsEncoder;
    private ReedSolomonDecoder rsDecoder;

    public BlockFixerHelper(Configuration conf) throws IOException {
      super(conf);

      xorPrefix = RaidNode.xorDestinationPath(getConf()).toUri().getPath();
      if (!xorPrefix.endsWith(Path.SEPARATOR)) {
        xorPrefix += Path.SEPARATOR;
      }
      rsPrefix = RaidNode.rsDestinationPath(getConf()).toUri().getPath();
      if (!rsPrefix.endsWith(Path.SEPARATOR)) {
        rsPrefix += Path.SEPARATOR;
      }
      int stripeLength = RaidNode.getStripeLength(getConf());
      xorEncoder = new XOREncoder1(getConf(), stripeLength);
      xorDecoder = new XORDecoder(getConf(), stripeLength);
      int parityLength = RaidNode.rsParityLength(getConf());
      rsEncoder = new ReedSolomonEncoder(getConf(), stripeLength, parityLength);
      rsDecoder = new ReedSolomonDecoder(getConf(), stripeLength, parityLength);

    }

    /**
     * checks whether file is xor parity file
     */
    boolean isXorParityFile(Path p) {
      String pathStr = p.toUri().getPath();
      return isXorParityFile(pathStr);
    }

    boolean isXorParityFile(String pathStr) {
      if (pathStr.contains(RaidNode.HAR_SUFFIX)) {
        return false;
      }
      return pathStr.startsWith(xorPrefix);
    }

    /**
     * checks whether file is rs parity file
     */
    boolean isRsParityFile(Path p) {
      String pathStr = p.toUri().getPath();
      return isRsParityFile(pathStr);
    }

    boolean isRsParityFile(String pathStr) {
      if (pathStr.contains(RaidNode.HAR_SUFFIX)) {
        return false;
      }
      return pathStr.startsWith(rsPrefix);
    }

    /**
     * Fix a file, report progess.
     *
     * @return true if file has been fixed, false if no fixing 
     * was necessary or possible.
     */
    boolean fixFile(Path srcPath, Progressable progress) throws IOException {

      if (RaidNode.isParityHarPartFile(srcPath)) {
        return processCorruptParityHarPartFile(srcPath, progress);
      }

      // The corrupted file is a XOR parity file
      if (isXorParityFile(srcPath)) {
        return processCorruptParityFile(srcPath, xorEncoder, progress);
      }

      // The corrupted file is a ReedSolomon parity file
      if (isRsParityFile(srcPath)) {
        return processCorruptParityFile(srcPath, rsEncoder, progress);
      }

      // The corrupted file is a source file
      ParityFilePair ppair = ParityFilePair.getParityFile(
          ErasureCodeType.XOR, srcPath, getConf());
      Decoder decoder = null;
      if (ppair != null) {
        decoder = xorDecoder;
      } else  {
        ppair = ParityFilePair.getParityFile(
            ErasureCodeType.RS, srcPath, getConf());
        if (ppair != null) {
          decoder = rsDecoder;
        }
      }

      // If we have a parity file, process the file and fix it.
      if (ppair != null) {
        return processCorruptFile(srcPath, ppair, decoder, progress);
      }
      
      // there was nothing to do
      return false;
    }

    /**
     * Sorts source files ahead of parity files.
     */
    void sortCorruptFiles(List<String> files) {
      // TODO: We should first fix the files that lose more blocks
      Comparator<String> comp = new Comparator<String>() {
        public int compare(String p1, String p2) {
          if (isXorParityFile(p2) || isRsParityFile(p2)) {
            // If p2 is a parity file, p1 is smaller.
            return -1;
          }
          if (isXorParityFile(p1) || isRsParityFile(p1)) {
            // If p1 is a parity file, p2 is smaller.
            return 1;
          }
          // If both are source files, they are equal.
          return 0;
        }
      };
      Collections.sort(files, comp);
    }

    /**
     * Returns a DistributedFileSystem hosting the path supplied.
     */
    protected DistributedFileSystem getDFS(Path p) throws IOException {
      return (DistributedFileSystem) p.getFileSystem(getConf());
    }

    /**
     * Reads through a corrupt source file fixing corrupt blocks on the way.
     * @param srcPath Path identifying the corrupt file.
     * @throws IOException
     * @return true if file has been fixed, false if no fixing 
     * was necessary or possible.
     */
    boolean processCorruptFile(Path srcPath, ParityFilePair parityPair,
                               Decoder decoder, Progressable progress)
      throws IOException {
      LOG.info("Processing corrupt file " + srcPath);

      DistributedFileSystem srcFs = getDFS(srcPath);
      FileStatus srcStat = srcFs.getFileStatus(srcPath);
      long blockSize = srcStat.getBlockSize();
      long srcFileSize = srcStat.getLen();
      String uriPath = srcPath.toUri().getPath();

      int numBlocksFixed = 0;
      List<LocatedBlock> corrupt = corruptBlocksInFile(srcFs, uriPath, srcStat);
      if (corrupt.size() == 0) {
        return false;
      }
      for (LocatedBlock lb: corrupt) {
        Block corruptBlock = lb.getBlock();
        long corruptOffset = lb.getStartOffset();

        LOG.info("Found corrupt block " + corruptBlock +
                 ", offset " + corruptOffset);

        final long blockContentsSize =
          Math.min(blockSize, srcFileSize - corruptOffset);
        File localBlockFile =
          File.createTempFile(corruptBlock.getBlockName(), ".tmp");
        localBlockFile.deleteOnExit();

        try {
          decoder.recoverBlockToFile(srcFs, srcPath, parityPair.getFileSystem(),
                                     parityPair.getPath(), blockSize,
                                     corruptOffset, localBlockFile,
                                     blockContentsSize, progress);

          // We have a the contents of the block, send them.
          String datanode = chooseDatanode(lb.getLocations());
          computeMetdataAndSendFixedBlock(datanode, localBlockFile,
                                          corruptBlock, blockContentsSize);
          numBlocksFixed++;

        } finally {
          localBlockFile.delete();
        }
        progress.progress();
      }
      LOG.info("Fixed " + numBlocksFixed + " blocks in " + srcPath);
      return true;
    }

    /**
     * Reads through a parity file, fixing corrupt blocks on the way.
     * This function uses the corresponding source file to regenerate parity
     * file blocks.
     * @return true if file has been fixed, false if no fixing 
     * was necessary or possible.
     */
    boolean processCorruptParityFile(Path parityPath, Encoder encoder, 
                                     Progressable progress)
      throws IOException {
      LOG.info("Processing corrupt file " + parityPath);
      Path srcPath = sourcePathFromParityPath(parityPath);
      if (srcPath == null) {
        LOG.warn("Unusable parity file " + parityPath);
        return false;
      }

      DistributedFileSystem parityFs = getDFS(parityPath);
      FileStatus parityStat = parityFs.getFileStatus(parityPath);
      long blockSize = parityStat.getBlockSize();
      FileStatus srcStat = getDFS(srcPath).getFileStatus(srcPath);
      long srcFileSize = srcStat.getLen();

      // Check timestamp.
      if (srcStat.getModificationTime() != parityStat.getModificationTime()) {
        LOG.info("Mismatching timestamp for " + srcPath + " and " + parityPath +
                 ", moving on...");
        return false;
      }

      String uriPath = parityPath.toUri().getPath();
      int numBlocksFixed = 0;
      List<LocatedBlock> corrupt = 
        corruptBlocksInFile(parityFs, uriPath, parityStat);
      if (corrupt.size() == 0) {
        return false;
      }
      for (LocatedBlock lb: corrupt) {
        Block corruptBlock = lb.getBlock();
        long corruptOffset = lb.getStartOffset();

        LOG.info("Found corrupt block " + corruptBlock +
                 ", offset " + corruptOffset);

        File localBlockFile =
          File.createTempFile(corruptBlock.getBlockName(), ".tmp");
        localBlockFile.deleteOnExit();

        try {
          encoder.recoverParityBlockToFile(parityFs, srcPath, srcFileSize,
                                           blockSize, parityPath, 
                                           corruptOffset, localBlockFile, progress);
          // We have a the contents of the block, send them.
          String datanode = chooseDatanode(lb.getLocations());
          computeMetdataAndSendFixedBlock(
                                          datanode, localBlockFile, 
                                          corruptBlock, blockSize);

          numBlocksFixed++;
        } finally {
          localBlockFile.delete();
        }
        progress.progress();
      }
      LOG.info("Fixed " + numBlocksFixed + " blocks in " + parityPath);
      return true;
    }

    /**
     * Reads through a parity HAR part file, fixing corrupt blocks on the way.
     * A HAR block can contain many file blocks, as long as the HAR part file
     * block size is a multiple of the file block size.
     * @return true if file has been fixed, false if no fixing 
     * was necessary or possible.
     */
    boolean processCorruptParityHarPartFile(Path partFile,
                                            Progressable progress)
      throws IOException {
      LOG.info("Processing corrupt file " + partFile);
      // Get some basic information.
      DistributedFileSystem dfs = getDFS(partFile);
      FileStatus partFileStat = dfs.getFileStatus(partFile);
      long partFileBlockSize = partFileStat.getBlockSize();
      LOG.info(partFile + " has block size " + partFileBlockSize);

      // Find the path to the index file.
      // Parity file HARs are only one level deep, so the index files is at the
      // same level as the part file.
      // Parses through the HAR index file.
      HarIndex harIndex = HarIndex.getHarIndex(dfs, partFile);
      String uriPath = partFile.toUri().getPath();
      int numBlocksFixed = 0;
      List<LocatedBlock> corrupt = corruptBlocksInFile(dfs, uriPath, 
                                                       partFileStat);
      if (corrupt.size() == 0) {
        return false;
      }
      for (LocatedBlock lb: corrupt) {
        Block corruptBlock = lb.getBlock();
        long corruptOffset = lb.getStartOffset();

        File localBlockFile =
          File.createTempFile(corruptBlock.getBlockName(), ".tmp");
        localBlockFile.deleteOnExit();
        processCorruptParityHarPartBlock(dfs, partFile, corruptBlock, 
                                         corruptOffset, partFileStat, harIndex,
                                         localBlockFile, progress);
        // Now we have recovered the part file block locally, send it.
        try {
          String datanode = chooseDatanode(lb.getLocations());
          computeMetdataAndSendFixedBlock(datanode, localBlockFile,
                                          corruptBlock, 
                                          localBlockFile.length());
          numBlocksFixed++;
        } finally {
          localBlockFile.delete();
        }
        progress.progress();
      }
      LOG.info("Fixed " + numBlocksFixed + " blocks in " + partFile);
      return true;
    }

    /**
     * This fixes a single part file block by recovering in sequence each
     * parity block in the part file block.
     * @return true if file has been fixed, false if no fixing 
     * was necessary or possible.
     */
    private void processCorruptParityHarPartBlock(FileSystem dfs, Path partFile,
                                                  Block corruptBlock, 
                                                  long corruptOffset,
                                                  FileStatus partFileStat,
                                                  HarIndex harIndex,
                                                  File localBlockFile,
                                                  Progressable progress)
      throws IOException {
      String partName = partFile.toUri().getPath(); // Temporarily.
      partName = partName.substring(1 + partName.lastIndexOf(Path.SEPARATOR));

      OutputStream out = new FileOutputStream(localBlockFile);

      try {
        // A HAR part file block could map to several parity files. We need to
        // use all of them to recover this block.
        final long corruptEnd = Math.min(corruptOffset + 
                                         partFileStat.getBlockSize(),
                                         partFileStat.getLen());
        for (long offset = corruptOffset; offset < corruptEnd; ) {
          HarIndex.IndexEntry entry = harIndex.findEntry(partName, offset);
          if (entry == null) {
            String msg = "Corrupt index file has no matching index entry for " +
              partName + ":" + offset;
            LOG.warn(msg);
            throw new IOException(msg);
          }
          Path parityFile = new Path(entry.fileName);
          Encoder encoder;
          if (isXorParityFile(parityFile)) {
            encoder = xorEncoder;
          } else if (isRsParityFile(parityFile)) {
            encoder = rsEncoder;
          } else {
            String msg = "Could not figure out parity file correctly";
            LOG.warn(msg);
            throw new IOException(msg);
          }
          Path srcFile = sourcePathFromParityPath(parityFile);
          FileStatus srcStat = dfs.getFileStatus(srcFile);
          if (srcStat.getModificationTime() != entry.mtime) {
            String msg = "Modification times of " + parityFile + " and " +
              srcFile + " do not match.";
            LOG.warn(msg);
            throw new IOException(msg);
          }
          long corruptOffsetInParity = offset - entry.startOffset;
          LOG.info(partFile + ":" + offset + " maps to " +
                   parityFile + ":" + corruptOffsetInParity +
                   " and will be recovered from " + srcFile);
          encoder.recoverParityBlockToStream(dfs, srcFile, srcStat.getLen(),
                                             srcStat.getBlockSize(), parityFile,
                                             corruptOffsetInParity, out, progress);
          // Finished recovery of one parity block. Since a parity block has the
          // same size as a source block, we can move offset by source block 
          // size.
          offset += srcStat.getBlockSize();
          LOG.info("Recovered " + srcStat.getBlockSize() + " part file bytes ");
          if (offset > corruptEnd) {
            String msg =
              "Recovered block spills across part file blocks. Cannot continue";
            throw new IOException(msg);
          }
          progress.progress();
        }
      } finally {
        out.close();
      }
    }

    /**
     * Choose a datanode (hostname:portnumber). The datanode is chosen at
     * random from the live datanodes.
     * @param locationsToAvoid locations to avoid.
     * @return A string in the format name:port.
     * @throws IOException
     */
    private String chooseDatanode(DatanodeInfo[] locationsToAvoid)
      throws IOException {
      DistributedFileSystem dfs = getDFS(new Path("/"));
      DatanodeInfo[] live =
        dfs.getClient().datanodeReport(DatanodeReportType.LIVE);
      LOG.info("Choosing a datanode from " + live.length +
               " live nodes while avoiding " + locationsToAvoid.length);
      Random rand = new Random();
      String chosen = null;
      int maxAttempts = 1000;
      for (int i = 0; i < maxAttempts && chosen == null; i++) {
        int idx = rand.nextInt(live.length);
        chosen = live[idx].name;
        for (DatanodeInfo avoid: locationsToAvoid) {
          if (chosen.equals(avoid.name)) {
            LOG.info("Avoiding " + avoid.name);
            chosen = null;
            break;
          }
        }
      }
      if (chosen == null) {
        throw new IOException("Could not choose datanode");
      }
      LOG.info("Choosing datanode " + chosen);
      return chosen;
    }

    /**
     * Reads data from the data stream provided and computes metadata.
     */
    DataInputStream computeMetadata(Configuration conf, InputStream dataStream)
      throws IOException {
      ByteArrayOutputStream mdOutBase = new ByteArrayOutputStream(1024*1024);
      DataOutputStream mdOut = new DataOutputStream(mdOutBase);

      // First, write out the version.
      mdOut.writeShort(FSDataset.METADATA_VERSION);

      // Create a summer and write out its header.
      int bytesPerChecksum = conf.getInt("io.bytes.per.checksum", 512);
      DataChecksum sum =
        DataChecksum.newDataChecksum(DataChecksum.CHECKSUM_CRC32,
                                     bytesPerChecksum);
      sum.writeHeader(mdOut);

      // Buffer to read in a chunk of data.
      byte[] buf = new byte[bytesPerChecksum];
      // Buffer to store the checksum bytes.
      byte[] chk = new byte[sum.getChecksumSize()];

      // Read data till we reach the end of the input stream.
      int bytesSinceFlush = 0;
      while (true) {
        // Read some bytes.
        int bytesRead = dataStream.read(buf, bytesSinceFlush, 
                                        bytesPerChecksum - bytesSinceFlush);
        if (bytesRead == -1) {
          if (bytesSinceFlush > 0) {
            boolean reset = true;
            sum.writeValue(chk, 0, reset); // This also resets the sum.
            // Write the checksum to the stream.
            mdOut.write(chk, 0, chk.length);
            bytesSinceFlush = 0;
          }
          break;
        }
        // Update the checksum.
        sum.update(buf, bytesSinceFlush, bytesRead);
        bytesSinceFlush += bytesRead;

        // Flush the checksum if necessary.
        if (bytesSinceFlush == bytesPerChecksum) {
          boolean reset = true;
          sum.writeValue(chk, 0, reset); // This also resets the sum.
          // Write the checksum to the stream.
          mdOut.write(chk, 0, chk.length);
          bytesSinceFlush = 0;
        }
      }

      byte[] mdBytes = mdOutBase.toByteArray();
      return new DataInputStream(new ByteArrayInputStream(mdBytes));
    }

    private void computeMetdataAndSendFixedBlock(String datanode,
                                                 File localBlockFile,
                                                 Block block, long blockSize)
      throws IOException {

      LOG.info("Computing metdata");
      InputStream blockContents = null;
      DataInputStream blockMetadata = null;
      try {
        blockContents = new FileInputStream(localBlockFile);
        blockMetadata = computeMetadata(getConf(), blockContents);
        blockContents.close();
        // Reopen
        blockContents = new FileInputStream(localBlockFile);
        sendFixedBlock(datanode, blockContents, blockMetadata, block, 
                       blockSize);
      } finally {
        if (blockContents != null) {
          blockContents.close();
          blockContents = null;
        }
        if (blockMetadata != null) {
          blockMetadata.close();
          blockMetadata = null;
        }
      }
    }

    /**
     * Send a generated block to a datanode.
     * @param datanode Chosen datanode name in host:port form.
     * @param blockContents Stream with the block contents.
     * @param corruptBlock Block identifying the block to be sent.
     * @param blockSize size of the block.
     * @throws IOException
     */
    private void sendFixedBlock(String datanode,
                                final InputStream blockContents,
                                DataInputStream metadataIn,
                                Block block, long blockSize) 
      throws IOException {
      InetSocketAddress target = NetUtils.createSocketAddr(datanode);
      Socket sock = SocketChannel.open().socket();

      int readTimeout =
        getConf().getInt(BLOCKFIX_READ_TIMEOUT, 
                         HdfsConstants.READ_TIMEOUT);
      NetUtils.connect(sock, target, readTimeout);
      sock.setSoTimeout(readTimeout);

      int writeTimeout = getConf().getInt(BLOCKFIX_WRITE_TIMEOUT,
                                          HdfsConstants.WRITE_TIMEOUT);
      
      OutputStream baseStream = NetUtils.getOutputStream(sock, writeTimeout);
      DataOutputStream out =
        new DataOutputStream(new BufferedOutputStream(baseStream, 
                                                      FSConstants.
                                                      SMALL_BUFFER_SIZE));

      boolean corruptChecksumOk = false;
      boolean chunkOffsetOK = false;
      boolean verifyChecksum = true;
      boolean transferToAllowed = false;

      try {
        LOG.info("Sending block " + block +
                 " from " + sock.getLocalSocketAddress().toString() +
                 " to " + sock.getRemoteSocketAddress().toString());
        BlockSender blockSender = 
          new BlockSender(block, blockSize, 0, blockSize,
                          corruptChecksumOk, chunkOffsetOK, verifyChecksum,
                          transferToAllowed,
                          metadataIn, new BlockSender.InputStreamFactory() {
                              @Override
                              public InputStream createStream(long offset) 
                                throws IOException {
                                // we are passing 0 as the offset above,
                                // so we can safely ignore
                                // the offset passed
                                return blockContents;
                              }
                            });

        // Header info
        out.writeShort(DataTransferProtocol.DATA_TRANSFER_VERSION);
        out.writeByte(DataTransferProtocol.OP_WRITE_BLOCK);
        out.writeLong(block.getBlockId());
        out.writeLong(block.getGenerationStamp());
        out.writeInt(0);           // no pipelining
        out.writeBoolean(false);   // not part of recovery
        Text.writeString(out, ""); // client
        out.writeBoolean(true); // sending src node information
        DatanodeInfo srcNode = new DatanodeInfo();
        srcNode.write(out); // Write src node DatanodeInfo
        // write targets
        out.writeInt(0); // num targets
        // send data & checksum
        blockSender.sendBlock(out, baseStream, null);

        LOG.info("Sent block " + block + " to " + datanode);
      } finally {
        sock.close();
        out.close();
      }
    }

    /**
     * returns the source file corresponding to a parity file
     */
    Path sourcePathFromParityPath(Path parityPath) {
      String parityPathStr = parityPath.toUri().getPath();
      if (parityPathStr.startsWith(xorPrefix)) {
        // Remove the prefix to get the source file.
        String src = parityPathStr.replaceFirst(xorPrefix, "/");
        return new Path(src);
      } else if (parityPathStr.startsWith(rsPrefix)) {
        // Remove the prefix to get the source file.
        String src = parityPathStr.replaceFirst(rsPrefix, "/");
        return new Path(src);
      }
      return null;
    }

    /**
     * Returns the corrupt blocks in a file.
     */
    static List<LocatedBlock> corruptBlocksInFile(DistributedFileSystem fs,
                                           String uriPath, FileStatus stat)
      throws IOException {
      List<LocatedBlock> corrupt = new LinkedList<LocatedBlock>();
      LocatedBlocks locatedBlocks =
        fs.getClient().namenode.getBlockLocations(uriPath, 0, stat.getLen());
      for (LocatedBlock b: locatedBlocks.getLocatedBlocks()) {
        if (b.isCorrupt() ||
            (b.getLocations().length == 0 && b.getBlockSize() > 0)) {
          corrupt.add(b);
        }
      }
      return corrupt;
    }

    static int numCorruptBlocksInFile(DistributedFileSystem fs,
                                         String uriPath, FileStatus stat)
        throws IOException {
      int num = 0;
      LocatedBlocks locatedBlocks =
        fs.getClient().namenode.getBlockLocations(uriPath, 0, stat.getLen());
      for (LocatedBlock b: locatedBlocks.getLocatedBlocks()) {
        if (b.isCorrupt() ||
            (b.getLocations().length == 0 && b.getBlockSize() > 0)) {
          num++;
        }
      }
      return num;
    }

  }

  public abstract Status getStatus();

  public static class Status {
    final int highPriorityFiles;
    final int lowPriorityFiles;
    final List<JobStatus> jobs;
    final List<String> highPriorityFileNames;
    final long lastUpdateTime;

    protected Status(int highPriorityFiles, int lowPriorityFiles,
        List<JobStatus> jobs, List<String> highPriorityFileNames) {
      this.highPriorityFiles = highPriorityFiles;
      this.lowPriorityFiles = lowPriorityFiles;
      this.jobs = jobs;
      this.highPriorityFileNames = highPriorityFileNames;
      this.lastUpdateTime = RaidNode.now();
    }

    @Override
    public String toString() {
      String result = BlockFixer.class.getSimpleName() + " Status:";
      result += " HighPriorityFiles:" + highPriorityFiles;
      result += " LowPriorityFiles:" + lowPriorityFiles;
      result += " Jobs:" + jobs.size();
      return result;
    }

    public String toHtml(boolean details) {
      long now = RaidNode.now();
      String html = "";
      html += tr(td("High Priority Corrupted Files") + td(":") +
                 td(StringUtils.humanReadableInt(highPriorityFiles)));
      html += tr(td("Low Priority Corrupted Files") + td(":") +
                 td(StringUtils.humanReadableInt(lowPriorityFiles)));
      html += tr(td("Running Jobs") + td(":") +
                 td(jobs.size() + ""));
      html += tr(td("Last Update") + td(":") +
                 td(StringUtils.formatTime(now - lastUpdateTime) + " ago"));
      html = JspUtils.tableSimple(html);
      if (!details) {
        return html;
      }

      if (jobs.size() > 0) {
        String jobTable = tr(JobStatus.htmlRowHeader());
        for (JobStatus job : jobs) {
          jobTable += tr(job.htmlRow());
        }
        jobTable = JspUtils.table(jobTable);
        html += "<br>" + jobTable;
      }

      if (highPriorityFileNames.size() > 0) {
        String highPriFilesTable = "";
        highPriFilesTable += tr(td("High Priority Corrupted Files") +
                                td(":") + td(highPriorityFileNames.get(0)));
        for (int i = 1; i < highPriorityFileNames.size(); ++i) {
          highPriFilesTable += tr(td("") + td(":") +
                                  td(highPriorityFileNames.get(i)));
        }
        highPriFilesTable = JspUtils.tableSimple(highPriFilesTable);
        html += "<br>" + highPriFilesTable;
      }

      return html;
    }
  }

  public static class JobStatus {
    final String id;
    final String name;
    final String url;
    JobStatus(JobID id, String name, String url) {
      this.id = id == null ? "" : id.toString();
      this.name = name == null ? "" : name;
      this.url = url == null ? "" : url;
    }
    @Override
    public String toString() {
      return "id:" + id + " name:" + name + " url:" + url;
    }
    public static String htmlRowHeader() {
      return td("JobID") + td("JobName");
    }
    public String htmlRow() {
      return td(JspUtils.link(id, url)) + td(name);
    }
  }
  private static String td(String s) {
    return JspUtils.td(s);
  }
  private static String tr(String s) {
    return JspUtils.tr(s);
  }
}

