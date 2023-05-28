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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Objects;

/**
 * {@link FetchConfig} represents the static configuration for fetching records from Kafka. It is simply a way
 * to bundle the immutable settings that were presented at the time the {@link Consumer} was created for later use by
 * classes like {@link Fetcher}, {@link CompletedFetch}, etc.
 *
 * <p/>
 *
 * In most cases, the values stored and returned by {@link FetchConfig} will be those stored in the following
 * {@link ConsumerConfig consumer configuration} settings:
 *
 * <ul>
 *     <li>{@link #minBytes}: {@link ConsumerConfig#FETCH_MIN_BYTES_CONFIG}</li>
 *     <li>{@link #maxBytes}: {@link ConsumerConfig#FETCH_MAX_BYTES_CONFIG}</li>
 *     <li>{@link #maxWaitMs}: {@link ConsumerConfig#FETCH_MAX_WAIT_MS_CONFIG}</li>
 *     <li>{@link #fetchSize}: {@link ConsumerConfig#MAX_PARTITION_FETCH_BYTES_CONFIG}</li>
 *     <li>{@link #maxPollRecords}: {@link ConsumerConfig#MAX_POLL_RECORDS_CONFIG}</li>
 *     <li>{@link #checkCrcs}: {@link ConsumerConfig#CHECK_CRCS_CONFIG}</li>
 *     <li>{@link #clientRackId}: {@link ConsumerConfig#CLIENT_RACK_CONFIG}</li>
 *     <li>{@link #keyDeserializer}: {@link ConsumerConfig#KEY_DESERIALIZER_CLASS_CONFIG}</li>
 *     <li>{@link #valueDeserializer}: {@link ConsumerConfig#VALUE_DESERIALIZER_CLASS_CONFIG}</li>
 *     <li>{@link #isolationLevel}: {@link ConsumerConfig#ISOLATION_LEVEL_CONFIG}</li>
 * </ul>
 *
 * However, there are places in the code where additional logic is used to determine these fetch-related configuration
 * values. In those cases, the values are calculated outside of this class and simply passed in when constructed.
 *
 * <p/>
 *
 * Note: the {@link Deserializer deserializers} used for the key and value are not closed by this class. They should be
 * closed by the creator of the {@link FetchConfig}.
 *
 * @param <K> Type used to {@link Deserializer deserialize} the message/record key
 * @param <V> Type used to {@link Deserializer deserialize} the message/record value
 */
public class FetchConfig<K, V> {
    // 一次消息拉取需要拉取的最小字节数，如果不组，会阻塞，默认值为1字节，如果增大这个值会增大吞吐，但会增加延迟，可以通参数 fetch.min.bytes 改变其默认值
    final int minBytes;
    /**
     * 一次消息拉取允许拉取的最大字节数，但这不是绝对的，如果一个分区的第一批记录超过了该值，也会返回。
     * 默认为50M,可通过参数 fetch.max.bytes 改变其默认值。
     * 同时不能超过 broker的配置参数(message.max.bytes) 和 主题级别的配置(max.message.bytes)
     * */
    final int maxBytes;
    // 在 broker 如果符合拉取条件的数据小于 minBytes 时阻塞的时间，默认为 500ms ，可通属性 fetch.max.wait.ms 进行定制
    final int maxWaitMs;
    // 每一个分区返回的最大消息字节数，如果分区中的第一批消息大于 fetchSize 也会返回
    final int fetchSize;
    // 单次拉取返回的最大记录数，默认值 500，可通过参数 max.poll.records 进行定制
    final int maxPollRecords;
    // 是否检查消息的 crcs 校验和，默认为 true，可通过参数 check.crcs 进行定制
    final boolean checkCrcs;
    // 客户端机架号
    final String clientRackId;
    // key反序列化器
    final Deserializer<K> keyDeserializer;
    // value反序列化器
    final Deserializer<V> valueDeserializer;
    // Kafka的隔离级别（与事务消息相关）
    final IsolationLevel isolationLevel;

    public FetchConfig(int minBytes,
                       int maxBytes,
                       int maxWaitMs,
                       int fetchSize,
                       int maxPollRecords,
                       boolean checkCrcs,
                       String clientRackId,
                       Deserializer<K> keyDeserializer,
                       Deserializer<V> valueDeserializer,
                       IsolationLevel isolationLevel) {
        this.minBytes = minBytes;
        this.maxBytes = maxBytes;
        this.maxWaitMs = maxWaitMs;
        this.fetchSize = fetchSize;
        this.maxPollRecords = maxPollRecords;
        this.checkCrcs = checkCrcs;
        this.clientRackId = clientRackId;
        this.keyDeserializer = Objects.requireNonNull(keyDeserializer, "Message key deserializer provided to FetchConfig should not be null");
        this.valueDeserializer = Objects.requireNonNull(valueDeserializer, "Message value deserializer provided to FetchConfig should not be null");
        this.isolationLevel = isolationLevel;
    }

    public FetchConfig(ConsumerConfig config,
                       Deserializer<K> keyDeserializer,
                       Deserializer<V> valueDeserializer,
                       IsolationLevel isolationLevel) {
        this.minBytes = config.getInt(ConsumerConfig.FETCH_MIN_BYTES_CONFIG);
        this.maxBytes = config.getInt(ConsumerConfig.FETCH_MAX_BYTES_CONFIG);
        this.maxWaitMs = config.getInt(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG);
        this.fetchSize = config.getInt(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG);
        this.maxPollRecords = config.getInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
        this.checkCrcs = config.getBoolean(ConsumerConfig.CHECK_CRCS_CONFIG);
        this.clientRackId = config.getString(ConsumerConfig.CLIENT_RACK_CONFIG);
        this.keyDeserializer = Objects.requireNonNull(keyDeserializer, "Message key deserializer provided to FetchConfig should not be null");
        this.valueDeserializer = Objects.requireNonNull(valueDeserializer, "Message value deserializer provided to FetchConfig should not be null");
        this.isolationLevel = isolationLevel;
    }

    @Override
    public String toString() {
        return "FetchConfig{" +
                "minBytes=" + minBytes +
                ", maxBytes=" + maxBytes +
                ", maxWaitMs=" + maxWaitMs +
                ", fetchSize=" + fetchSize +
                ", maxPollRecords=" + maxPollRecords +
                ", checkCrcs=" + checkCrcs +
                ", clientRackId='" + clientRackId + '\'' +
                ", keyDeserializer=" + keyDeserializer +
                ", valueDeserializer=" + valueDeserializer +
                ", isolationLevel=" + isolationLevel +
                '}';
    }
}
