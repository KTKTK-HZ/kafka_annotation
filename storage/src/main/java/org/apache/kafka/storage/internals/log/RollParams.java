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

/**
 * A class used to hold params required to decide to rotate a log segment or not.
 * 用于控制日志段是否切分（Roll）的数据结构。
 */
public class RollParams {

    public final long maxSegmentMs; // 日志段保留的最大时间，默认168小时，由参数log.roll.hours设置
    public final int maxSegmentBytes; // 日志段最大byte数
    public final long maxTimestampInMessages; // 待写入消息中的最大时间戳
    public final long maxOffsetInMessages; // 待写入消息的最大位移值
    public final int messagesSize; // 待写入消息段的大小
    public final long now; // 当前时间

    public RollParams(long maxSegmentMs,
               int maxSegmentBytes,
               long maxTimestampInMessages,
               long maxOffsetInMessages,
               int messagesSize,
               long now) {

        this.maxSegmentMs = maxSegmentMs;
        this.maxSegmentBytes = maxSegmentBytes;
        this.maxTimestampInMessages = maxTimestampInMessages;
        this.maxOffsetInMessages = maxOffsetInMessages;
        this.messagesSize = messagesSize;
        this.now = now;
    }

    @Override
    public String toString() {
        return "RollParams(" +
                "maxSegmentMs=" + maxSegmentMs +
                ", maxSegmentBytes=" + maxSegmentBytes +
                ", maxTimestampInMessages=" + maxTimestampInMessages +
                ", maxOffsetInMessages=" + maxOffsetInMessages +
                ", messagesSize=" + messagesSize +
                ", now=" + now +
                ')';
    }

}
