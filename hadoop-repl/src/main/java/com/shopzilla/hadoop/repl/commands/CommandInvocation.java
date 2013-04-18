/**
 * Copyright 2012 Shopzilla.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  http://tech.shopzilla.com
 *
 */
package com.shopzilla.hadoop.repl.commands;

import java.util.Arrays;

/**
 * @author Jeremy Lucas
 * @since 4/11/13
 */
public class CommandInvocation {
    protected final String command;
    protected final String[] args;

    public CommandInvocation(final String command, final String... args) {
        this.command = command;
        this.args = Arrays.copyOf(args, args.length);
    }

    public String command() {
        return command;
    }

    public String[] args() {
        return Arrays.copyOf(args, args.length);
    }
}
