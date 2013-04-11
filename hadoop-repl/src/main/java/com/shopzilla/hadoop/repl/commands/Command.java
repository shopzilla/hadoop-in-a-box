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

import com.shopzilla.hadoop.repl.REPL;
import com.shopzilla.hadoop.repl.SessionState;

/**
 * @author Jeremy Lucas
 * @since 4/11/13
 */
public interface Command {
    void execute(final CommandInvocation call, final SessionState sessionState) throws REPL.ExitSignal;

    Usage usage(final SessionState sessionState);

    public static class Usage {
        public final String command;
        public final String description;
        public final String[] arguments;

        public Usage(final String command, final String description, final String... arguments) {
            this.command = command;
            this.description = description;
            this.arguments = arguments;
        }
    }
}
