package org.apache.hadoop.hdfs.server.datanode.erasurecode;

import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.server.protocol.BlockECRecoveryCommand.BlockECRecoveryInfo;
import org.apache.hadoop.io.erasurecode.coder.AbstractErasureCoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RSRawDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RSRawEncoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureCoder;

/**
 * ErasureCodingWorker handles the erasure coding recovery work commands. These
 * commands would be issued from Namenode as part of Datanode's heart beat
 * response. BPOfferService delegates the work to this class for handling EC
 * commands.
 */
public final class ErasureCodingWorker {

  private Configuration conf;
  RawErasureCoder rawEncoder = null;
  RawErasureCoder rawDecoder = null;

  public ErasureCodingWorker(Configuration conf) {
    this.conf = conf;
    initialize();
  }

  /**
   * Initializes the required resources for handling the erasure coding recovery
   * work.
   */
  public void initialize() {
    // Right now directly used RS coder. Once other coders integration ready, we
    // can load preferred codec here.
    initializeErasureEncoder();
    initializeErasureDecoder();
  }

  private void initializeErasureDecoder() {
    rawDecoder = AbstractErasureCoder.createRawCoder(conf,
        CommonConfigurationKeys.IO_ERASURECODE_CODEC_RS_RAWCODER_KEY, false);
    if (rawDecoder == null) {
      rawDecoder = new RSRawDecoder();
    }
  }

  private void initializeErasureEncoder() {
    rawEncoder = AbstractErasureCoder.createRawCoder(conf,
        CommonConfigurationKeys.IO_ERASURECODE_CODEC_RS_RAWCODER_KEY, true);
    if (rawEncoder == null) {
      rawEncoder = new RSRawEncoder();
    }
  }

  /**
   * Handles the Erasure Coding recovery work commands.
   * 
   * @param ecTasks
   *          BlockECRecoveryInfo
   */
  public void processErasureCodingTasks(Collection<BlockECRecoveryInfo> ecTasks) {
    // HDFS-7348 : Implement the actual recovery process
  }
}
