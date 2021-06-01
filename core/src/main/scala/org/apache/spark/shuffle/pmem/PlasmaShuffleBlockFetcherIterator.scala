/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.pmem

import java.io.{InputStream, IOException}
import java.nio.channels.ClosedByInterruptException
import java.util.concurrent.LinkedBlockingQueue
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Queue}

import org.apache.commons.io.IOUtils

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle._
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.{BlockException, BlockId, BlockManager, BlockManagerId}
import org.apache.spark.storage.{FallbackStorage, ShuffleBlockBatchId, ShuffleBlockId}
import org.apache.spark.util.{CompletionIterator, TaskCompletionListener, Utils}

private[spark] final class PlasmaShuffleBlockFetcherIterator(
    context: TaskContext,
    shuffleClient: BlockStoreClient,
    blockManager: BlockManager,
    blocksByAddress: Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])],
    streamWrapper: (BlockId, InputStream) => InputStream,
    maxBytesInFlight: Long,
    maxReqsInFlight: Int,
    maxBlocksInFlightPerAddress: Int,
    detectCorrupt: Boolean)
  extends Iterator[(BlockId, InputStream)] with Logging {

  import PlasmaShuffleBlockFetcherIterator._

  private val targetRemoteRequestSize = math.max(maxBytesInFlight / 5, 1L)

  private[this] var numBlocksToFetch = 0

  private[this] var numBlocksProcessed = 0

  private[this] val startTimeNs = System.nanoTime()

  private[this] val localBlocks = scala.collection.mutable.LinkedHashSet[(BlockId, Int)]()

  private[this] val results = new LinkedBlockingQueue[PlasmaFetchResult]

  @volatile private[this] var currentResult: SuccessPlasmaFetchResult = null

  private[this] val fetchRequests = new Queue[PlasmaFetchRequest]

  private[this] val deferredFetchRequests = new HashMap[BlockManagerId, Queue[PlasmaFetchRequest]]()

  private[this] var bytesInFlight = 0L

  private[this] var reqsInFlight = 0

  private[this] val numBlocksInFlightPerAddress = new HashMap[BlockManagerId, Int]()

  private[this] val corruptedBlocks = mutable.HashSet[BlockId]()

  @GuardedBy("this")
  private[this] var isZombie = false

  private[this] val onCompleteCallback = new PlasmaShuffleFetchCompletionListener(this)

  initialize()

  private[pmem] def releaseCurrentResultBuffer(): Unit = {
    if (currentResult != null) {
      currentResult.buf.release()
    }
    currentResult = null
  }

  private[pmem] def cleanup(): Unit = {
    synchronized {
      isZombie = true
    }
    releaseCurrentResultBuffer()
    // Release buffers in the results queue
    val iter = results.iterator()
    while (iter.hasNext) {
      val result = iter.next()
      result match {
        case SuccessPlasmaFetchResult(_, _, _, _, buf, _) =>
          buf.release()
        case _ =>
      }
    }
  }

  private[this] def sendRequest(req: PlasmaFetchRequest): Unit = {
    logDebug("Sending request for %d blocks (%s) from %s".format(
      req.blocks.size, Utils.bytesToString(req.size), req.address.hostPort))
    bytesInFlight += req.size
    reqsInFlight += 1

    val infoMap = req.blocks.map {
      case PlasmaFetchBlockInfo(blockId, size, mapIndex) => (blockId.toString, (size, mapIndex))
    }.toMap
    val remainingBlocks = new HashSet[String]() ++= infoMap.keys
    val blockIds = req.blocks.map(_.blockId.toString)
    val address = req.address

    val blockFetchingListener = new BlockFetchingListener {
      override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
        // Only add the buffer to results queue if the iterator is not zombie,
        // i.e. cleanup() has not been called yet.
        PlasmaShuffleBlockFetcherIterator.this.synchronized {
          if (!isZombie) {
            // Increment the ref count because we need to pass this to a different thread.
            // This needs to be released after use.
            buf.retain()
            remainingBlocks -= blockId
            results.put(new SuccessPlasmaFetchResult(BlockId(blockId), infoMap(blockId)._2,
              address, infoMap(blockId)._1, buf, remainingBlocks.isEmpty))
            logDebug("remainingBlocks: " + remainingBlocks)
          }
        }
        logTrace(s"Got remote block $blockId after ${Utils.getUsedTimeNs(startTimeNs)}")
      }

      override def onBlockFetchFailure(blockId: String, e: Throwable): Unit = {
        logError(s"Failed to get block(s) from ${req.address.host}:${req.address.port}", e)
        results.put(FailurePlasmaFetchResult(BlockId(blockId), infoMap(blockId)._2, address, e))
      }
    }

    shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
      blockFetchingListener, null)
  }

  private[this] def partitionBlocksByFetchMode(): ArrayBuffer[PlasmaFetchRequest] = {
    logDebug(s"maxBytesInFlight: $maxBytesInFlight, targetRemoteRequestSize: "
      + s"$targetRemoteRequestSize, maxBlocksInFlightPerAddress: $maxBlocksInFlightPerAddress")

    val collectedRemoteRequests = new ArrayBuffer[PlasmaFetchRequest]
    var localBlockBytes = 0L
    var remoteBlockBytes = 0L

    val fallback = FallbackStorage.FALLBACK_BLOCK_MANAGER_ID.executorId
    for ((address, blockInfos) <- blocksByAddress) {
      if (Seq(blockManager.blockManagerId.executorId, fallback).contains(address.executorId)) {
        checkBlockSizes(blockInfos)
        val fetchBlockInfos = blockInfos.map(
          info => PlasmaFetchBlockInfo(info._1, info._2, info._3)
        )
        numBlocksToFetch += fetchBlockInfos.size
        localBlocks ++= fetchBlockInfos.map(info => (info.blockId, info.mapIndex))
        localBlockBytes += fetchBlockInfos.map(_.size).sum
      } else {
        remoteBlockBytes += blockInfos.map(_._2).sum
        collectFetchRequests(address, blockInfos, collectedRemoteRequests)
      }
//      remoteBlockBytes += blockInfos.map(_._2).sum
//      collectFetchRequests(address, blockInfos, collectedRemoteRequests)
    }
    val numRemoteBlocks = collectedRemoteRequests.map(_.blocks.size).sum
    val totalBytes = localBlockBytes + remoteBlockBytes
    assert(numBlocksToFetch == localBlocks.size + numRemoteBlocks,
      s"The number of non-empty blocks $numBlocksToFetch doesn't equal to the number of local " +
        s"blocks ${localBlocks.size} " +
        s"+ the number of remote blocks ${numRemoteBlocks}.")
    logInfo(s"Getting $numBlocksToFetch (${Utils.bytesToString(totalBytes)}) non-empty blocks " +
      s"including ${localBlocks.size} (${Utils.bytesToString(localBlockBytes)}) local and " +
      s"$numRemoteBlocks (${Utils.bytesToString(remoteBlockBytes)}) remote blocks")
    collectedRemoteRequests
  }

  private def createPlasmaFetchRequest(
      blocks: Seq[PlasmaFetchBlockInfo],
      address: BlockManagerId): PlasmaFetchRequest = {
    logDebug(s"Creating fetch request of ${blocks.map(_.size).sum} at $address "
      + s"with ${blocks.size} blocks")
    PlasmaFetchRequest(address, blocks)
  }

  private def createPlasmaFetchRequests(
      curBlocks: Seq[PlasmaFetchBlockInfo],
      address: BlockManagerId,
      isLast: Boolean,
      collectedRemoteRequests: ArrayBuffer[PlasmaFetchRequest]): Seq[PlasmaFetchBlockInfo] = {
    numBlocksToFetch += curBlocks.size
    var retBlocks = Seq.empty[PlasmaFetchBlockInfo]
    if (curBlocks.length <= maxBlocksInFlightPerAddress) {
      collectedRemoteRequests += createPlasmaFetchRequest(curBlocks, address)
    } else {
      curBlocks.grouped(maxBlocksInFlightPerAddress).foreach { blocks =>
        if (blocks.length == maxBlocksInFlightPerAddress || isLast) {
          collectedRemoteRequests += createPlasmaFetchRequest(blocks, address)
        } else {
          // The last group does not exceed `maxBlocksInFlightPerAddress`. Put it back
          // to `curBlocks`.
          retBlocks = blocks
          numBlocksToFetch -= blocks.size
        }
      }
    }
    retBlocks
  }

  private def collectFetchRequests(
      address: BlockManagerId,
      blockInfos: Seq[(BlockId, Long, Int)],
      collectedRemoteRequests: ArrayBuffer[PlasmaFetchRequest]): Unit = {
    val iterator = blockInfos.iterator
    var curRequestSize = 0L
    var curBlocks = Seq.empty[PlasmaFetchBlockInfo]

    while (iterator.hasNext) {
      val (blockId, size, mapIndex) = iterator.next()
      assertPositiveBlockSize(blockId, size)
      curBlocks = curBlocks ++ Seq(PlasmaFetchBlockInfo(blockId, size, mapIndex))
      curRequestSize += size
      val mayExceedsMaxBlocks = curBlocks.size >= maxBlocksInFlightPerAddress
      if (curRequestSize >= targetRemoteRequestSize || mayExceedsMaxBlocks) {
        curBlocks = createPlasmaFetchRequests(curBlocks, address, isLast = false,
          collectedRemoteRequests)
        curRequestSize = curBlocks.map(_.size).sum
      }
    }
    // Add in the final request
    if (curBlocks.nonEmpty) {
      curBlocks = createPlasmaFetchRequests(curBlocks, address, isLast = true,
        collectedRemoteRequests)
      curRequestSize = curBlocks.map(_.size).sum
    }
  }

  private def assertPositiveBlockSize(blockId: BlockId, blockSize: Long): Unit = {
    if (blockSize < 0) {
      throw BlockException(blockId, "Negative block size " + size)
    } else if (blockSize == 0) {
      throw BlockException(blockId, "Zero-sized blocks should be excluded.")
    }
  }

  private def checkBlockSizes(blockInfos: Seq[(BlockId, Long, Int)]): Unit = {
    blockInfos.foreach { case (blockId, size, _) => assertPositiveBlockSize(blockId, size) }
  }

  private[this] def fetchLocalBlocks(): Unit = {
    logDebug(s"Start fetching local blocks: ${localBlocks.mkString(", ")}")
    val iter = localBlocks.iterator
    while (iter.hasNext) {
      val (blockId, mapIndex) = iter.next()
      try {
        val buf = blockManager.getLocalBlockData(blockId)
        buf.retain()
        results.put(SuccessPlasmaFetchResult(blockId, mapIndex, blockManager.blockManagerId,
          buf.size(), buf, false))
      } catch {
        // If we see an exception, stop immediately.
        case e: Exception =>
          e match {
            // ClosedByInterruptException is an excepted exception when kill task,
            // don't log the exception stack trace to avoid confusing users.
            // See: SPARK-28340
            case ce: ClosedByInterruptException =>
              logError("Error occurred while fetching local blocks, " + ce.getMessage)
            case ex: Exception => logError("Error occurred while fetching local blocks", ex)
          }
          results.put(FailurePlasmaFetchResult(blockId, mapIndex, blockManager.blockManagerId, e))
          return
      }
    }
  }

  private[this] def initialize(): Unit = {
    // Add a task completion callback (called in both success case and failure case) to cleanup.
    context.addTaskCompletionListener(onCompleteCallback)

    // Partition blocks by the different fetch modes: local, host-local and remote blocks.
    val remoteRequests = partitionBlocksByFetchMode()
    // Add the remote requests into our queue in a random order
    fetchRequests ++= Utils.randomize(remoteRequests)
    assert ((0 == reqsInFlight) == (0 == bytesInFlight),
      "expected reqsInFlight = 0 but found reqsInFlight = " + reqsInFlight +
        ", expected bytesInFlight = 0 but found bytesInFlight = " + bytesInFlight)

    // Send out initial requests for blocks, up to our maxBytesInFlight
    fetchUpToMaxBytes()

    val numDeferredRequest = deferredFetchRequests.values.map(_.size).sum
    val numFetches = remoteRequests.size - fetchRequests.size - numDeferredRequest
    logInfo(s"Started $numFetches remote fetches in ${Utils.getUsedTimeNs(startTimeNs)}" +
      (if (numDeferredRequest > 0 ) s", deferred $numDeferredRequest requests" else ""))

    // Get Local Blocks
    fetchLocalBlocks()
    logDebug(s"Got local blocks in ${Utils.getUsedTimeNs(startTimeNs)}")
  }

  override def hasNext: Boolean = numBlocksProcessed < numBlocksToFetch

  override def next(): (BlockId, InputStream) = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }

    numBlocksProcessed += 1

    var result: PlasmaFetchResult = null
    var input: InputStream = null
    while (result == null) {
      result = results.take()
      result match {
        case r @ SuccessPlasmaFetchResult(
            blockId, mapIndex, address, size, buf, isNetworkReqDone) =>
          if (address != blockManager.blockManagerId) {
            numBlocksInFlightPerAddress(address) = numBlocksInFlightPerAddress(address) - 1
            bytesInFlight -= size
          }
          if (isNetworkReqDone) {
            reqsInFlight -= 1
            logDebug("Number of requests in flight " + reqsInFlight)
          }

          if (buf.size == 0) {
            val msg = s"Received a zero-size buffer for block $blockId from $address " +
              s"(expectedApproxSize = $size, isNetworkReqDone=$isNetworkReqDone)"
            throwFetchFailedException(blockId, mapIndex, address, new IOException(msg))
          }

          val in = try {
            buf.createInputStream()
          } catch {
            case e: IOException =>
              assert(buf.isInstanceOf[PlasmaShuffleManagedBuffer])
              e match {
                case ce: ClosedByInterruptException =>
                  logError("Failed to create input stream from local block, " +
                    ce.getMessage)
                case e: IOException => logError("Failed to create input stream from local block", e)
              }
              buf.release()
              throwFetchFailedException(blockId, mapIndex, address, e)
          }
          try {
            input = streamWrapper(blockId, in)
          } catch {
            case e: IOException =>
              buf.release()
              if (buf.isInstanceOf[PlasmaShuffleManagedBuffer]
                || corruptedBlocks.contains(blockId)) {
                throwFetchFailedException(blockId, mapIndex, address, e)
              } else {
                logWarning(s"got an corrupted block $blockId from $address, fetch again", e)
                corruptedBlocks += blockId
                fetchRequests += PlasmaFetchRequest(
                  address, Array(PlasmaFetchBlockInfo(blockId, size, mapIndex)))
                result = null
              }
          } finally {
            // TODO: release the buf here to free memory earlier
            if (input == null) {
              // Close the underlying stream if there was an issue in wrapping the stream using
              // streamWrapper
              in.close()
            }
          }

        case FailurePlasmaFetchResult(blockId, mapIndex, address, e) =>
          throwFetchFailedException(blockId, mapIndex, address, e)
      }

      // Send fetch requests up to maxBytesInFlight
      fetchUpToMaxBytes()
    }

    currentResult = result.asInstanceOf[SuccessPlasmaFetchResult]
    (currentResult.blockId,
      new PlasmaBufferReleasingInputStream(
        input,
        this,
        currentResult.blockId,
        currentResult.mapIndex,
        currentResult.address,
        detectCorrupt))
  }

  def toCompletionIterator: Iterator[(BlockId, InputStream)] = {
    CompletionIterator[(BlockId, InputStream), this.type](this,
      onCompleteCallback.onComplete(context))
  }

  private def fetchUpToMaxBytes(): Unit = {
    // Send fetch requests up to maxBytesInFlight. If you cannot fetch from a remote host
    // immediately, defer the request until the next time it can be processed.

    // Process any outstanding deferred fetch requests if possible.
    if (deferredFetchRequests.nonEmpty) {
      for ((remoteAddress, defReqQueue) <- deferredFetchRequests) {
        while (isRemoteBlockFetchable(defReqQueue) &&
          !isRemoteAddressMaxedOut(remoteAddress, defReqQueue.front)) {
          val request = defReqQueue.dequeue()
          logDebug(s"Processing deferred fetch request for $remoteAddress with "
            + s"${request.blocks.length} blocks")
          send(remoteAddress, request)
          if (defReqQueue.isEmpty) {
            deferredFetchRequests -= remoteAddress
          }
        }
      }
    }

    // Process any regular fetch requests if possible.
    while (isRemoteBlockFetchable(fetchRequests)) {
      val request = fetchRequests.dequeue()
      val remoteAddress = request.address
      if (isRemoteAddressMaxedOut(remoteAddress, request)) {
        logDebug(s"Deferring fetch request for $remoteAddress with ${request.blocks.size} blocks")
        val defReqQueue = deferredFetchRequests.getOrElse(
          remoteAddress, new Queue[PlasmaFetchRequest]())
        defReqQueue.enqueue(request)
        deferredFetchRequests(remoteAddress) = defReqQueue
      } else {
        send(remoteAddress, request)
      }
    }

    def send(remoteAddress: BlockManagerId, request: PlasmaFetchRequest): Unit = {
      sendRequest(request)
      numBlocksInFlightPerAddress(remoteAddress) =
        numBlocksInFlightPerAddress.getOrElse(remoteAddress, 0) + request.blocks.size
    }

    def isRemoteBlockFetchable(fetchReqQueue: Queue[PlasmaFetchRequest]): Boolean = {
      fetchReqQueue.nonEmpty &&
        (bytesInFlight == 0 ||
          (reqsInFlight + 1 <= maxReqsInFlight &&
            bytesInFlight + fetchReqQueue.front.size <= maxBytesInFlight))
    }

    // Checks if sending a new fetch request will exceed the max no. of blocks being fetched from a
    // given remote address.
    def isRemoteAddressMaxedOut(
        remoteAddress: BlockManagerId,
        request: PlasmaFetchRequest): Boolean = {
      numBlocksInFlightPerAddress.getOrElse(remoteAddress, 0) + request.blocks.size >
        maxBlocksInFlightPerAddress
    }
  }

  private[pmem] def throwFetchFailedException(
      blockId: BlockId,
      mapIndex: Int,
      address: BlockManagerId,
      e: Throwable) = {
    blockId match {
      case ShuffleBlockId(shufId, mapId, reduceId) =>
        throw new FetchFailedException(address, shufId, mapId, mapIndex, reduceId, e)
      case ShuffleBlockBatchId(shuffleId, mapId, startReduceId, _) =>
        throw new FetchFailedException(address, shuffleId, mapId, mapIndex, startReduceId, e)
      case _ =>
        throw new SparkException(
          "Failed to get block " + blockId + ", which is not a shuffle block", e)
    }
  }
}

private class PlasmaBufferReleasingInputStream(
    // This is visible for testing
    private[pmem] val delegate: InputStream,
    private val iterator: PlasmaShuffleBlockFetcherIterator,
    private val blockId: BlockId,
    private val mapIndex: Int,
    private val address: BlockManagerId,
    private val detectCorruption: Boolean)
  extends InputStream {
  private[this] var closed = false

  override def read(): Int =
    tryOrFetchFailedException(delegate.read())

  override def close(): Unit = {
    if (!closed) {
      delegate.close()
      iterator.releaseCurrentResultBuffer()
      closed = true
    }
  }

  override def available(): Int = delegate.available()

  override def mark(readlimit: Int): Unit = delegate.mark(readlimit)

  override def skip(n: Long): Long =
    tryOrFetchFailedException(delegate.skip(n))

  override def markSupported(): Boolean = delegate.markSupported()

  override def read(b: Array[Byte]): Int =
    tryOrFetchFailedException(delegate.read(b))

  override def read(b: Array[Byte], off: Int, len: Int): Int =
    tryOrFetchFailedException(delegate.read(b, off, len))

  override def reset(): Unit = delegate.reset()

  private def tryOrFetchFailedException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException if detectCorruption =>
        IOUtils.closeQuietly(this)
        iterator.throwFetchFailedException(blockId, mapIndex, address, e)
    }
  }
}

private class PlasmaShuffleFetchCompletionListener(var data: PlasmaShuffleBlockFetcherIterator)
  extends TaskCompletionListener {

  override def onTaskCompletion(context: TaskContext): Unit = {
    if (data != null) {
      data.cleanup()
      // Null out the referent here to make sure we don't keep a reference to this
      // ShuffleBlockFetcherIterator, after we're done reading from it, to let it be
      // collected during GC. Otherwise we can hold metadata on block locations(blocksByAddress)
      data = null
    }
  }

  // Just an alias for onTaskCompletion to avoid confusing
  def onComplete(context: TaskContext): Unit = this.onTaskCompletion(context)
}

private[pmem] object PlasmaShuffleBlockFetcherIterator {

  private[pmem] case class PlasmaFetchBlockInfo(
      blockId: BlockId,
      size: Long,
      mapIndex: Int)

  case class PlasmaFetchRequest(address: BlockManagerId, blocks: Seq[PlasmaFetchBlockInfo]) {
    val size = blocks.map(_.size).sum
  }

  private[pmem] sealed trait PlasmaFetchResult {
    val blockId: BlockId
    val address: BlockManagerId
  }

  private[pmem] case class SuccessPlasmaFetchResult(
      blockId: BlockId,
      mapIndex: Int,
      address: BlockManagerId,
      size: Long,
      buf: ManagedBuffer,
      isNetworkReqDone: Boolean) extends PlasmaFetchResult {
    require(buf != null)
    require(size >= 0)
  }

  private[pmem] case class FailurePlasmaFetchResult(
      blockId: BlockId,
      mapIndex: Int,
      address: BlockManagerId,
      e: Throwable)
    extends PlasmaFetchResult
}
