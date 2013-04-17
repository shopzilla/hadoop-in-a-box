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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.shopzilla.hadoop.repl.commands.completers.HDFSFileNameCompletor;
import com.shopzilla.hadoop.repl.SessionState;
import jline.console.completer.FileNameCompleter;
import org.apache.hadoop.fs.FsShell;

import java.util.Map;

import static com.shopzilla.hadoop.repl.commands.Call.call;

/**
 * @author Jeremy Lucas
 * @since 4/11/13
 */
public class FSShellCommandProvider implements CommandProvider {  

    @Override
    public Map<Call, Command> apply(final SessionState sessionState) {
        final Iterable<Call> REPL_COMMANDS = ImmutableSet.<Call>builder()
            .add(call("ls", new HDFSFileNameCompletor(sessionState.configuration)))
            .add(call("lsr", new HDFSFileNameCompletor(sessionState.configuration)))
            .add(call("df", new HDFSFileNameCompletor(sessionState.configuration)))
            .add(call("du", new HDFSFileNameCompletor(sessionState.configuration)))
            .add(call("dus", new HDFSFileNameCompletor(sessionState.configuration)))
            .add(call("count", new HDFSFileNameCompletor(sessionState.configuration)))
            .add(call("mv", new HDFSFileNameCompletor(sessionState.configuration), new HDFSFileNameCompletor(sessionState.configuration)))
            .add(call("cp", new HDFSFileNameCompletor(sessionState.configuration), new HDFSFileNameCompletor(sessionState.configuration)))
            .add(call("rm", new HDFSFileNameCompletor(sessionState.configuration)))
            .add(call("rmr", new HDFSFileNameCompletor(sessionState.configuration)))
            .add(call("expunge"))
            .add(call("put", new FileNameCompleter(), new HDFSFileNameCompletor(sessionState.configuration)))
            .add(call("cat", new HDFSFileNameCompletor(sessionState.configuration)))
            .add(call("text", new HDFSFileNameCompletor(sessionState.configuration)))
            .add(call("copyToLocal", new HDFSFileNameCompletor(sessionState.configuration), new FileNameCompleter()))
            .add(call("moveToLocal", new HDFSFileNameCompletor(sessionState.configuration), new FileNameCompleter()))
            .add(call("mkdir", new HDFSFileNameCompletor(sessionState.configuration)))
//            .add(call("setrep"))
            .add(call("touchz", new HDFSFileNameCompletor(sessionState.configuration)))
            .add(call("stat", new HDFSFileNameCompletor(sessionState.configuration)))
            .add(call("tail", new HDFSFileNameCompletor(sessionState.configuration)))
//            .add(call("chmod"))
//            .add(call("chown"))
//            .add(call("chgrp"))
            .add(call("copyFromLocal", new FileNameCompleter(), new HDFSFileNameCompletor(sessionState.configuration)))
            .add(call("moveFromLocal", new FileNameCompleter(), new HDFSFileNameCompletor(sessionState.configuration)))
            .add(call("get", new HDFSFileNameCompletor(sessionState.configuration), new FileNameCompleter()))
            .add(call("getmerge", new HDFSFileNameCompletor(sessionState.configuration), new FileNameCompleter()))
            .build();
        final ImmutableMap.Builder<Call, Command> commandMappingBuilder = new ImmutableMap.Builder<Call, Command>();
        for (final Call call : REPL_COMMANDS) {
            commandMappingBuilder.put(call, new Command() {
                @Override
                public void execute(final CommandInvocation call, final SessionState sessionState) {
                    try {
                        new FsShell(sessionState.configuration).run(Joiner.on(" ").join("-" + call.command, Joiner.on(" ").join(call.args)).split(" "));
                    } catch (final Exception ex) {
                        sessionState.error(ex);
                    }
                }

                @Override
                public Usage usage(final SessionState sessionState) {
                    return new Usage(
                        call.commandName,
                        "" // TODO: Set this up!
                    );
                }
            });
        }
        return commandMappingBuilder.build();
    }
}
