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
package org.apache.kafka.server.util;

import joptsimple.AbstractOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

public abstract class CommandDefaultOptions {
    public final String[] args; // 将命令行传入的args存储起来
    public final OptionParser parser; // 选项解析器
    public final AbstractOptionSpec<Void> helpOpt;
    public final AbstractOptionSpec<Void> versionOpt;
    public OptionSet options;

    public CommandDefaultOptions(String[] args) {
        this(args, false);
    }

    public CommandDefaultOptions(String[] args, boolean allowCommandOptionAbbreviation) {
        this.args = args;
        this.parser = new OptionParser(allowCommandOptionAbbreviation); // 允许选项缩写
        // forHelp()的作用是把forHelp属性设置为true
        this.helpOpt = parser.accepts("help", "Print usage information.").forHelp();
        this.versionOpt = parser.accepts("version", "Display Kafka version.").forHelp();
        this.options = null;
    }
}
