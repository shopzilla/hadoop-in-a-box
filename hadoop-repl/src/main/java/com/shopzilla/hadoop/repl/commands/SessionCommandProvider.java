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

import com.google.common.collect.ImmutableMap;
import com.shopzilla.hadoop.repl.REPL;
import com.shopzilla.hadoop.repl.SessionState;
import jline.console.completer.FileNameCompleter;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import static com.shopzilla.hadoop.repl.commands.Call.call;
import static java.lang.String.format;

/**
 * @author Jeremy Lucas
 * @since 4/11/13
 */
public class SessionCommandProvider implements CommandProvider {

    private final Command QUIT_COMMAND = new Command() {
        @Override
        public void execute(final CommandInvocation call, final SessionState sessionState) throws REPL.ExitSignal {
            sessionState.shutdown();
        }

        @Override
        public Usage usage(final SessionState sessionState) {
            return new Usage(
                "quit | exit",
                "Disconnects your current REPL session. If you are running in standalone mode, this will delete all unsaved cluster state."
            );
        }
    };

    private final Map<Call, Command> REPL_COMMANDS = ImmutableMap.<Call, Command>builder()
        .put(call("history"), new Command() {
            @Override
            public void execute(final CommandInvocation call, final SessionState sessionState) throws REPL.ExitSignal {
                int i = 1;
                for (final String item : sessionState.history()) {
                    sessionState.outputColumns(8, "[" + i++ + "]:", item);
                }
            }

            @Override
            public Usage usage(final SessionState sessionState) {
                return new Usage(
                    "history",
                    "Shows a history of recently executed commands from the current REPL session"
                );
            }
        })
        .put(call("save", new FileNameCompleter()), new Command() {

            private final DateFormat DF = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");

            @Override
            public void execute(final CommandInvocation call, final SessionState sessionState) throws REPL.ExitSignal {
                if (call.args.length == 0) {
                    sessionState.saveClusterState(format("session-%s.tgz", DF.format(new Date())));
                } else if (call.args.length == 1) {
                    sessionState.saveClusterState(call.args[0]);
                } else {
                    sessionState.outputUsage(this);
                }
            }

            @Override
            public Usage usage(SessionState sessionState) {
                return new Usage(
                    "save",
                    "Saves the current session's cluster state to disk",
                    "<path-to-save-cluster-state>"
                );
            }
        })
        .put(call("load", new FileNameCompleter()), new Command() {
            @Override
            public void execute(final CommandInvocation call, final SessionState sessionState) throws REPL.ExitSignal {
                if (call.args.length != 1) {
                    sessionState.outputUsage(this);
                } else {
                    sessionState.loadClusterState(call.args[0]);
                }
            }

            @Override
            public Usage usage(SessionState sessionState) {
                return new Usage(
                    "load",
                    "Loads the current session's cluster state from disk",
                    "<path-to-load-cluster-state>"
                );
            }
        })
        .put(call("quit"), QUIT_COMMAND)
        .put(call("exit"), QUIT_COMMAND)
        .build();

    @Override
    public Map<Call, Command> apply(final SessionState input) {
        return REPL_COMMANDS;
    }
}
