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
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.StringUtils;

/**
 * This class fixes source file blocks using the parity file,
 * and parity file blocks using the source file.
 * It periodically fetches the list of corrupt files from the namenode,
 * and figures out the location of the bad block by reading through
 * the corrupt file.
 */
public class LocalBlockFixer extends BlockFixer {
  public static final Log LOG = LogFactory.getLog(LocalBlockFixer.class);

  private BlockFixerHelper helper;

  public LocalBlockFixer(Configuration conf) throws IOException {
    super(conf);
    helper = new BlockFixerHelper(getConf());
  }

  public void run() {
    while (running) {
      try {
        LOG.info("LocalBlockFixer continuing to run...");
        doFix();
      } catch (Exception e) {
        LOG.error(StringUtils.stringifyException(e));
      } catch (Error err) {
        LOG.error("Exiting after encountering " +
                    StringUtils.stringifyException(err));
        throw err;
      }
    }
  }

  void doFix() throws InterruptedException, IOException {
    while (running) {
      // Sleep before proceeding to fix files.
      Thread.sleep(blockFixInterval);

      List<String> corruptFiles = getCorruptFiles();
      FileSystem parityFs = new Path("/").getFileSystem(getConf());
      filterUnfixableSourceFiles(parityFs, corruptFiles.iterator());
      RaidNodeMetrics.getInstance().numFilesToFix.set(corruptFiles.size());

      if (corruptFiles.isEmpty()) {
        // If there are no corrupt files, retry after some time.
        continue;
      }
      LOG.info("Found " + corruptFiles.size() + " corrupt files.");

      helper.sortCorruptFiles(corruptFiles);

      for (String srcPath: corruptFiles) {
        if (!running) break;
        try {
          boolean fixed = helper.fixFile(new Path(srcPath), RaidUtils.NULL_PROGRESSABLE);
          if (fixed) {
            incrFilesFixed();
          }
        } catch (IOException ie) {
          incrFileFixFailures();
          LOG.error("Hit error while processing " + srcPath +
            ": " + StringUtils.stringifyException(ie));
          // Do nothing, move on to the next file.
        }
      }
    }
  }

  /**
   * @return A list of corrupt files as obtained from the namenode
   */
  List<String> getCorruptFiles() throws IOException {
    DistributedFileSystem dfs = helper.getDFS(new Path("/"));

    String[] files = DFSUtil.getCorruptFiles(dfs);
    List<String> corruptFiles = new LinkedList<String>();
    for (String f: files) {
      corruptFiles.add(f);
    }
    RaidUtils.filterTrash(getConf(), corruptFiles);
    return corruptFiles;
  }

  @Override
  public BlockFixer.Status getStatus() {
    throw new UnsupportedOperationException(LocalBlockFixer.class +
        " doesn't do getStatus()");
  }

}

