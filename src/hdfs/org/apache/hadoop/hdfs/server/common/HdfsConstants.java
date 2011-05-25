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
package org.apache.hadoop.hdfs.server.common;


/************************************
 * Some handy internal HDFS constants
 *
 ************************************/

public interface HdfsConstants {
  /**
   * Type of the node
   */
  static public enum NodeType {
    NAME_NODE,
    DATA_NODE;
  }

  // Startup options
  static public enum StartupOption{
    FORMAT  ("-format"),
    REGULAR ("-regular"),
    UPGRADE ("-upgrade"),
    ROLLBACK("-rollback"),
    FINALIZE("-finalize"),
    IMPORT  ("-importCheckpoint");
    
    private String name = null;
    private StartupOption(String arg) {this.name = arg;}
    public String getName() {return name;}
  }

  // Timeouts for communicating with DataNode for streaming writes/reads
  public static int READ_TIMEOUT = 60 * 1000;
  public static int WRITE_TIMEOUT = 8 * 60 * 1000;
  public static int WRITE_TIMEOUT_EXTENSION = 5 * 1000; //for write pipeline

  // constants for edits log
  public static long DEFAULT_EDIT_PREALLOCATE_SIZE = 1024 * 1024; // 1 MB
  public static int  DEFAULT_EDIT_BUFFER_SIZE = 512 * 1024; // 0.5 MB
  public static int  DEFAULT_MAX_BUFFERED_TRANSACTIONS = 10000; // ten 

  // property for fsimage compression
  public static final String DFS_IMAGE_COMPRESS_KEY = "dfs.image.compress";
  public static final boolean DFS_IMAGE_COMPRESS_DEFAULT = false;
  public static final String DFS_IMAGE_COMPRESSION_CODEC_KEY = 
    "dfs.image.compression.codec";
  public static final String DFS_IMAGE_COMPRESSION_CODEC_DEFAULT =
    "org.apache.hadoop.io.compress.DefaultCodec";
  public static final String DFS_IMAGE_TRANSFER_RATE_KEY =
    "dfs.image.transfer.bandwidthPerSec";
  public static final long DFS_IMAGE_TRANSFER_RATE_DEFAULT = 0;  // disable
  public static final String DFS_IMAGE_SAVE_ON_START_KEY =
    "dfs.image.save.on.start";
  public static final boolean DFS_IMAGE_SAVE_ON_START_DEFAULT = true;
}

