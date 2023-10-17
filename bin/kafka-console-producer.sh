#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if [ "x$KAFKA_HEAP_OPTS" = "x" ]; then
    export KAFKA_HEAP_OPTS="-Xmx512M" # 在脚本中通过export设置，该环境变量是临时的，仅在当前脚本的生命周期内有效
fi
exec $(dirname $0)/kafka-run-class.sh kafka.tools.ConsoleProducer "$@"
# "$@"表示将外部传来的参数原样传给ConsoleProducer主类
# exec命令有两个常见用途:
  #1、用一个新进程替换当前进程，上面的用法就是这种形式：
      #这里会启动kafka-run-class.sh,并用它替换当前shell进程。这意味着kafka-run-class.sh执行完成后,shell进程也会退出,不会回到脚本中继续执行。
      #如果不用exec,那么脚本会等待kafka-run-class.sh完成,然后继续往下执行。
  #2、第二个用途是改变文件描述符,常见是让stderr合并到stdout，例如exec 2>&1
  #这条命令的作用是将标准错误重定向到标准输出，2代表标准错误文件描述符(stderr)，1代表标准输出文件描述符(stdout)，&表示重定向
  #some_cmd > output.log 2>&1 这里some_cmd的标准输出会输出到output.log，同时stderr也被重定向到stdout,最终output.log中包含了两者的输出。
  #如果没有2>&1, stderr会单独输出到终端,这时日志中会丢失stderr信息。