/**
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

package kafka.coordinator.group

import java.util

import kafka.utils.nonthreadsafe

// 组成员概要数据，提取了最核心的元数据信息。bin/kafka-consumer-groups.sh --describe就是返回的该数据
// 相当于一个POJO类，仅仅承载数据，没有定义任何逻辑
case class MemberSummary(memberId: String, // 成员ID，由Kafka自动生成，规则是consumer-组ID-<序号>
                         groupInstanceId: Option[String], // Consumer端参数group.instance.id值，消费者组静态成员的ID
                         clientId: String, // client.id参数值，由于memberId不能被设置，因此，你可以用这个字段来区分消费者组下的不同成员。
                         clientHost: String, // consumer端程序主机名，它记录了这个客户端是从哪台机器发出的消费请求。
                         metadata: Array[Byte], // 消费者组成员使用的分配策略，由消费者端参数partition.assignment.strategy值设定
                         assignment: Array[Byte]) // 成员订阅分区

// 仅仅定义了一个方法，供上层组件调用，从一组给定的分区分配策略详情中提取出分区分配策略的名称，并将其封装成一个集合对象，然后返回
// 使用Set返回是因为Set内没有重复值
private object MemberMetadata {
  def plainProtocolSet(supportedProtocols: List[(String, Array[Byte])]) = supportedProtocols.map(_._1).toSet
}

/**
 * Member metadata contains the following metadata:
 *
 * Heartbeat metadata:
 * 1. negotiated heartbeat session timeout
 * 2. timestamp of the latest heartbeat
 *
 * Protocol metadata:
 * 1. the list of supported protocols (ordered by preference)
 * 2. the metadata associated with each protocol
 *
 * In addition, it also contains the following state information:
 *
 * 1. Awaiting rebalance callback: when the group is in the prepare-rebalance state,
 *                                 its rebalance callback will be kept in the metadata if the
 *                                 member has sent the join group request
 * 2. Awaiting sync callback: when the group is in the awaiting-sync state, its sync callback
 *                            is kept in metadata until the leader provides the group assignment
 *                            and the group transitions to stable
 */
// 消费者组成员的元数据，Kafka为消费者组成员定义了很多数据。
@nonthreadsafe
private[group] class MemberMetadata(var memberId: String,
                                    val groupInstanceId: Option[String],
                                    val clientId: String,
                                    val clientHost: String,
                                    var rebalanceTimeoutMs: Int, // Rebalance操作超时时间，即消费者的max.poll.interval.ms
                                    var sessionTimeoutMs: Int, // 会话超时时间
                                    val protocolType: String, // 对消费者组而言，是"consumer"，Kafka connect中的消费者类型为connect
                                    var supportedProtocols: List[(String, Array[Byte])], // 成员配置的多套分区分配策略
                                    var assignment: Array[Byte] = Array.empty[Byte] /**分区分配方案*/) {

  var awaitingJoinCallback: JoinGroupResult => Unit = _ // 成员入组的回调函数
  var awaitingSyncCallback: SyncGroupResult => Unit = _ // 组成员等待GroupCoordinator发送分配方案的回调函数
  var isNew: Boolean = false // 表示是否是消费者组下的新成员

  def isStaticMember: Boolean = groupInstanceId.isDefined

  // This variable is used to track heartbeat completion through the delayed
  // heartbeat purgatory. When scheduling a new heartbeat expiration, we set
  // this value to `false`. Upon receiving the heartbeat (or any other event
  // indicating the liveness of the client), we set it to `true` so that the
  // delayed heartbeat can be completed.
  var heartbeatSatisfied: Boolean = false

  def isAwaitingJoin: Boolean = awaitingJoinCallback != null
  def isAwaitingSync: Boolean = awaitingSyncCallback != null

  /**
   * Get metadata corresponding to the provided protocol.
   * 从该成员配置的分区分配方案列表中寻找给定策略的详情。如果找到，就直接返回详情字节数组数据，否则，就抛出异常
   */
  def metadata(protocol: String): Array[Byte] = {
    supportedProtocols.find(_._1 == protocol) match {
      case Some((_, metadata)) => metadata
      case None =>
        throw new IllegalArgumentException("Member does not support protocol")
    }
  }

  // 检查心跳
  def hasSatisfiedHeartbeat: Boolean = {
    if (isNew) {
      // 新成员在等待时可能会过期，所以先进行一次检查
      heartbeatSatisfied
    } else if (isAwaitingJoin || isAwaitingSync) {
      // Members that are awaiting a rebalance automatically satisfy expected heartbeats
      true
    } else {
      // Otherwise we require the next heartbeat
      heartbeatSatisfied
    }
  }

  /**
   * Check if the provided protocol metadata matches the currently stored metadata.
   * 检查提供的协议元数据是否与当前存储的元数据匹配。
   */
  def matches(protocols: List[(String, Array[Byte])]): Boolean = {
    if (protocols.size != this.supportedProtocols.size)
      return false

    for (i <- protocols.indices) {
      val p1 = protocols(i)
      val p2 = supportedProtocols(i)
      if (p1._1 != p2._1 || !util.Arrays.equals(p1._2, p2._2))
        return false
    }
    true
  }

  def summary(protocol: String): MemberSummary = {
    MemberSummary(memberId, groupInstanceId, clientId, clientHost, metadata(protocol), assignment)
  }

  def summaryNoMetadata(): MemberSummary = {
    MemberSummary(memberId, groupInstanceId, clientId, clientHost, Array.empty[Byte], Array.empty[Byte])
  }

  /**
   * Vote for one of the potential group protocols. This takes into account the protocol preference as
   * indicated by the order of supported protocols and returns the first one also contained in the set
   */
  def vote(candidates: Set[String]): String = {
    supportedProtocols.find({ case (protocol, _) => candidates.contains(protocol)}) match {
      case Some((protocol, _)) => protocol
      case None =>
        throw new IllegalArgumentException("Member does not support any of the candidate protocols")
    }
  }

  override def toString: String = {
    "MemberMetadata(" +
      s"memberId=$memberId, " +
      s"groupInstanceId=$groupInstanceId, " +
      s"clientId=$clientId, " +
      s"clientHost=$clientHost, " +
      s"sessionTimeoutMs=$sessionTimeoutMs, " +
      s"rebalanceTimeoutMs=$rebalanceTimeoutMs, " +
      s"supportedProtocols=${supportedProtocols.map(_._1)}" +
      ")"
  }
}
