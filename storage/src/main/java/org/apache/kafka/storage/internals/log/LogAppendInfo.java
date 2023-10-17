/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.RecordConversionStats;
import org.apache.kafka.common.requests.ProduceResponse.RecordError;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * Struct to hold various quantities we compute about each message set before appending to the log.
 * 保存一组待写入消息的元数据，比如，这组消息中第一条消息的位移值是多少、最后一条消息的位移值是多少，这组消息中最大的消息时间戳又是多少等。
 */
public class LogAppendInfo {

    public static final LogAppendInfo UNKNOWN_LOG_APPEND_INFO = new LogAppendInfo(Optional.empty(), -1, OptionalInt.empty(),
            RecordBatch.NO_TIMESTAMP, -1L, RecordBatch.NO_TIMESTAMP, -1L,
            RecordConversionStats.EMPTY, CompressionType.NONE, CompressionType.NONE, -1, -1,
            false, -1L);

    private Optional<LogOffsetMetadata> firstOffset; // 消息集中的第一个偏移量，如果该量存在，其类型为LogOffsetMetadata
    private long lastOffset; // 消息集中的最后一个偏移量
    private long maxTimestamp; // 消息集中的最大时间戳
    private long offsetOfMaxTimestamp; // 最大时间戳所对应消息的偏移量
    private long logAppendTime; // 消息集添加到日志的时间，否则为Message.NoTimestamp
    private long logStartOffset; // 添加消息集时log的起始偏移量
    private RecordConversionStats recordConversionStats; // record处理期间收集的统计信息，如果“assignOffsets”为“false”，则为“null”

    private final OptionalInt lastLeaderEpoch; // 与最后一个offset对应的lastLeaderEpoch
    private final CompressionType sourceCompression; // 生产者生产时采用的压缩格式
    private final CompressionType targetCompression; // 消息集的目标编解码器
    private final int shallowCount; //shallow message的数量
    private final int validBytes; // 有效字节数
    private final boolean offsetsMonotonic; // 该消息集中的偏移量是否单调递增
    private final long lastOffsetOfFirstBatch; // 第一批的最后一个偏移量
    private final List<RecordError> recordErrors; // 导致相应批次被删除的记录错误列表
    private final String errorMessage; // 错误信息
    private final LeaderHwChange leaderHwChange; // 如果追加记录后需要增加高水印，则增量。如果不更改高水印，则相同。 None 是默认值，表示追加失败

    /**
     * Creates an instance with the given params.
     *
     * @param firstOffset            The first offset in the message set unless the message format is less than V2 and we are appending
     *                               to the follower. If the message is a duplicate message the segment base offset and relative position
     *                               in segment will be unknown.
     * @param lastOffset             The last offset in the message set
     * @param lastLeaderEpoch        The partition leader epoch corresponding to the last offset, if available.
     * @param maxTimestamp           The maximum timestamp of the message set.
     * @param offsetOfMaxTimestamp   The offset of the message with the maximum timestamp.
     * @param logAppendTime          The log append time (if used) of the message set, otherwise Message.NoTimestamp
     * @param logStartOffset         The start offset of the log at the time of this append.
     * @param recordConversionStats  Statistics collected during record processing, `null` if `assignOffsets` is `false`
     * @param sourceCompression      The source codec used in the message set (send by the producer)
     * @param targetCompression      The target codec of the message set(after applying the broker compression configuration if any)
     * @param shallowCount           The number of shallow messages
     * @param validBytes             The number of valid bytes
     * @param offsetsMonotonic       Are the offsets in this message set monotonically increasing
     * @param lastOffsetOfFirstBatch The last offset of the first batch
     */
    public LogAppendInfo(Optional<LogOffsetMetadata> firstOffset,
                         long lastOffset,
                         OptionalInt lastLeaderEpoch,
                         long maxTimestamp,
                         long offsetOfMaxTimestamp,
                         long logAppendTime,
                         long logStartOffset,
                         RecordConversionStats recordConversionStats,
                         CompressionType sourceCompression,
                         CompressionType targetCompression,
                         int shallowCount,
                         int validBytes,
                         boolean offsetsMonotonic,
                         long lastOffsetOfFirstBatch) {
        this(firstOffset, lastOffset, lastLeaderEpoch, maxTimestamp, offsetOfMaxTimestamp, logAppendTime, logStartOffset,
                recordConversionStats, sourceCompression, targetCompression, shallowCount, validBytes, offsetsMonotonic,
                lastOffsetOfFirstBatch, Collections.<RecordError>emptyList(), null, LeaderHwChange.NONE);
    }

    /**
     * Creates an instance with the given params.
     *
     * @param firstOffset            The first offset in the message set unless the message format is less than V2 and we are appending
     *                               to the follower. If the message is a duplicate message the segment base offset and relative position
     *                               in segment will be unknown.
     * @param lastOffset             The last offset in the message set
     * @param lastLeaderEpoch        The partition leader epoch corresponding to the last offset, if available.
     * @param maxTimestamp           The maximum timestamp of the message set.
     * @param offsetOfMaxTimestamp   The offset of the message with the maximum timestamp.
     * @param logAppendTime          The log append time (if used) of the message set, otherwise Message.NoTimestamp
     * @param logStartOffset         The start offset of the log at the time of this append.
     * @param recordConversionStats  Statistics collected during record processing, `null` if `assignOffsets` is `false`
     * @param sourceCompression      The source codec used in the message set (send by the producer)
     * @param targetCompression      The target codec of the message set(after applying the broker compression configuration if any)
     * @param shallowCount           The number of shallow messages
     * @param validBytes             The number of valid bytes
     * @param offsetsMonotonic       Are the offsets in this message set monotonically increasing
     * @param lastOffsetOfFirstBatch The last offset of the first batch
     * @param errorMessage           error message
     * @param recordErrors           List of record errors that caused the respective batch to be dropped
     * @param leaderHwChange         Incremental if the high watermark needs to be increased after appending record
     *                               Same if high watermark is not changed. None is the default value and it means append failed
     */
    public LogAppendInfo(Optional<LogOffsetMetadata> firstOffset,
                         long lastOffset,
                         OptionalInt lastLeaderEpoch,
                         long maxTimestamp,
                         long offsetOfMaxTimestamp,
                         long logAppendTime,
                         long logStartOffset,
                         RecordConversionStats recordConversionStats,
                         CompressionType sourceCompression,
                         CompressionType targetCompression,
                         int shallowCount,
                         int validBytes,
                         boolean offsetsMonotonic,
                         long lastOffsetOfFirstBatch,
                         List<RecordError> recordErrors,
                         String errorMessage,
                         LeaderHwChange leaderHwChange) {
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
        this.lastLeaderEpoch = lastLeaderEpoch;
        this.maxTimestamp = maxTimestamp;
        this.offsetOfMaxTimestamp = offsetOfMaxTimestamp;
        this.logAppendTime = logAppendTime;
        this.logStartOffset = logStartOffset;
        this.recordConversionStats = recordConversionStats;
        this.sourceCompression = sourceCompression;
        this.targetCompression = targetCompression;
        this.shallowCount = shallowCount;
        this.validBytes = validBytes;
        this.offsetsMonotonic = offsetsMonotonic;
        this.lastOffsetOfFirstBatch = lastOffsetOfFirstBatch;
        this.recordErrors = recordErrors;
        this.errorMessage = errorMessage;
        this.leaderHwChange = leaderHwChange;
    }

    public Optional<LogOffsetMetadata> firstOffset() {
        return firstOffset;
    }

    public void setFirstOffset(Optional<LogOffsetMetadata> firstOffset) {
        this.firstOffset = firstOffset;
    }

    public long lastOffset() {
        return lastOffset;
    }

    public void setLastOffset(long lastOffset) {
        this.lastOffset = lastOffset;
    }

    public OptionalInt lastLeaderEpoch() {
        return lastLeaderEpoch;
    }

    public long maxTimestamp() {
        return maxTimestamp;
    }

    public void setMaxTimestamp(long maxTimestamp) {
        this.maxTimestamp = maxTimestamp;
    }

    public long offsetOfMaxTimestamp() {
        return offsetOfMaxTimestamp;
    }

    public void setOffsetOfMaxTimestamp(long offsetOfMaxTimestamp) {
        this.offsetOfMaxTimestamp = offsetOfMaxTimestamp;
    }

    public long logAppendTime() {
        return logAppendTime;
    }

    public void setLogAppendTime(long logAppendTime) {
        this.logAppendTime = logAppendTime;
    }

    public long logStartOffset() {
        return logStartOffset;
    }

    public void setLogStartOffset(long logStartOffset) {
        this.logStartOffset = logStartOffset;
    }

    public RecordConversionStats recordConversionStats() {
        return recordConversionStats;
    }

    public void setRecordConversionStats(RecordConversionStats recordConversionStats) {
        this.recordConversionStats = recordConversionStats;
    }

    public CompressionType sourceCompression() {
        return sourceCompression;
    }

    public CompressionType targetCompression() {
        return targetCompression;
    }

    public int shallowCount() {
        return shallowCount;
    }

    public int validBytes() {
        return validBytes;
    }

    public boolean offsetsMonotonic() {
        return offsetsMonotonic;
    }

    public List<RecordError> recordErrors() {
        return recordErrors;
    }

    public String errorMessage() {
        return errorMessage;
    }

    public LeaderHwChange leaderHwChange() {
        return leaderHwChange;
    }

    /**
     * Get the first offset if it exists, else get the last offset of the first batch
     * For magic versions 2 and newer, this method will return first offset. For magic versions
     * older than 2, we use the last offset of the first batch as an approximation of the first
     * offset to avoid decompressing the data.
     */
    public long firstOrLastOffsetOfFirstBatch() {
        return firstOffset.map(x -> x.messageOffset).orElse(lastOffsetOfFirstBatch);
    }

    /**
     * Get the (maximum) number of messages described by LogAppendInfo
     * 利用firstOffset和lastOffset这两个变量,在合法范围内计算了消息数
     * @return Maximum possible number of messages described by LogAppendInfo
     */
    public long numMessages() {
        if (firstOffset.isPresent() && firstOffset.get().messageOffset >= 0 && lastOffset >= 0) {
            return lastOffset - firstOffset.get().messageOffset + 1;
        }
        return 0;
    }

    /**
     * Returns a new instance containing all the fields from this instance and updated with the given LeaderHwChange value.
     *
     * @param newLeaderHwChange new value for LeaderHwChange
     * @return a new instance with the given LeaderHwChange
     */
    public LogAppendInfo copy(LeaderHwChange newLeaderHwChange) {
        return new LogAppendInfo(firstOffset, lastOffset, lastLeaderEpoch, maxTimestamp, offsetOfMaxTimestamp, logAppendTime, logStartOffset, recordConversionStats,
                sourceCompression, targetCompression, shallowCount, validBytes, offsetsMonotonic, lastOffsetOfFirstBatch, recordErrors, errorMessage, newLeaderHwChange);
    }

    public static LogAppendInfo unknownLogAppendInfoWithLogStartOffset(long logStartOffset) {
        return new LogAppendInfo(Optional.empty(), -1, OptionalInt.empty(), RecordBatch.NO_TIMESTAMP, -1L, RecordBatch.NO_TIMESTAMP, logStartOffset,
                RecordConversionStats.EMPTY, CompressionType.NONE, CompressionType.NONE, -1, -1,
                false, -1L);
    }

    /**
     * In ProduceResponse V8+, we add two new fields record_errors and error_message (see KIP-467).
     * For any record failures with InvalidTimestamp or InvalidRecordException, we construct a LogAppendInfo object like the one
     * in unknownLogAppendInfoWithLogStartOffset, but with additional fields recordErrors and errorMessage
     */
    public static LogAppendInfo unknownLogAppendInfoWithAdditionalInfo(long logStartOffset, List<RecordError> recordErrors, String errorMessage) {
        return new LogAppendInfo(Optional.empty(), -1, OptionalInt.empty(), RecordBatch.NO_TIMESTAMP, -1L, RecordBatch.NO_TIMESTAMP, logStartOffset,
                RecordConversionStats.EMPTY, CompressionType.NONE, CompressionType.NONE, -1, -1,
                false, -1L, recordErrors, errorMessage, LeaderHwChange.NONE);
    }

    @Override
    public String toString() {
        return "LogAppendInfo(" +
                "firstOffset=" + firstOffset +
                ", lastOffset=" + lastOffset +
                ", lastLeaderEpoch=" + lastLeaderEpoch +
                ", maxTimestamp=" + maxTimestamp +
                ", offsetOfMaxTimestamp=" + offsetOfMaxTimestamp +
                ", logAppendTime=" + logAppendTime +
                ", logStartOffset=" + logStartOffset +
                ", recordConversionStats=" + recordConversionStats +
                ", sourceCompression=" + sourceCompression +
                ", targetCompression=" + targetCompression +
                ", shallowCount=" + shallowCount +
                ", validBytes=" + validBytes +
                ", offsetsMonotonic=" + offsetsMonotonic +
                ", lastOffsetOfFirstBatch=" + lastOffsetOfFirstBatch +
                ", recordErrors=" + recordErrors +
                ", errorMessage='" + errorMessage + '\'' +
                ", leaderHwChange=" + leaderHwChange +
                ')';
    }
}
