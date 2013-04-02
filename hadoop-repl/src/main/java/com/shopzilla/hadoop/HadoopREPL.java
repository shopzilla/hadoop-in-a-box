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

package com.shopzilla.hadoop;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import jline.ArgumentCompletor;
import jline.MultiCompletor;
import jline.SimpleCompletor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static java.lang.String.format;

/**
 * @author Jeremy Lucas
 * @since 9/12/12
 */
public class HadoopREPL extends REPL {

    private final Configuration configuration;

    private final FileSystem fs;

    private final FsShell shell;

    private final CommandFunction FS_SHELL_COMMAND = new CommandFunction() {
        @Override
        public String execute(final String command, final String[] args) {
            try {
                shell.run(Joiner.on(" ").join("-" + command, Joiner.on(" ").join(args)).split(" "));
            } catch (final Exception ex) {
                System.err.println(ex);
            }
            return null;
        }
    };

    private final CommandFunction QUIT_COMMAND = new CommandFunction() {
        @Override
        public String execute(final String command, final String[] args) {
            throw new ExitSignal(0, "Disconnecting Hadoop REPL...");
        }
    };

    private final Map<String, CommandFunction> REPL_COMMANDS = ImmutableMap.<String, CommandFunction>builder()
        // TODO: Get this to work! :)
//        .put("submit", new CommandFunction() {
//            @Override
//            public String execute(final String command, final String[] args) throws ExitSignal {
//                try {
//                    final int jobExitCode = ToolRunner.run(configuration, new JobClient(), args);
//                    if (jobExitCode != 0) {
//                        return format("Job failed with exit code [%s]", jobExitCode);
//                    } else {
//                        return format("Job complete");
//                    }
//                } catch (final Exception ex) {
//                    throw new ExitSignal(100, ex.getMessage());
//                }
//
//            }
//        })
        .put("help", new CommandFunction() {
            @Override
            public String execute(final String command, final String[] args) throws ExitSignal {
                if (args.length != 1) {
                    return "Usage: help [command]";
                } else {
                    if (REPL_COMMANDS.containsKey(args[0])) {
                        try {
                            shell.run(new String[]{ "-help", args[0] });
                        } catch (final Exception ex) {
                            System.err.println(ex);
                        }
                        return format("Displayed help for \"%s\":\n", args[0]);
                    } else {
                        return format("Unknown command \"%s\"", args[0]);
                    }
                }
            }
        })
        .put("quit", QUIT_COMMAND)
        .put("exit", QUIT_COMMAND)
        .put("ls", FS_SHELL_COMMAND)
        .put("lsr", FS_SHELL_COMMAND)
        .put("df", FS_SHELL_COMMAND)
        .put("du", FS_SHELL_COMMAND)
        .put("dus", FS_SHELL_COMMAND)
        .put("count", FS_SHELL_COMMAND)
        .put("mv", FS_SHELL_COMMAND)
        .put("cp", FS_SHELL_COMMAND)
        .put("rm", FS_SHELL_COMMAND)
        .put("rmr", FS_SHELL_COMMAND)
        .put("expunge", FS_SHELL_COMMAND)
        .put("put", FS_SHELL_COMMAND)
        .put("cat", FS_SHELL_COMMAND)
        .put("text", FS_SHELL_COMMAND)
        .put("copyToLocal", FS_SHELL_COMMAND)
        .put("moveToLocal", FS_SHELL_COMMAND)
        .put("mkdir", FS_SHELL_COMMAND)
        .put("setrep", FS_SHELL_COMMAND)
        .put("touchz", FS_SHELL_COMMAND)
        .put("stat", FS_SHELL_COMMAND)
        .put("tail", FS_SHELL_COMMAND)
        .put("chmod", FS_SHELL_COMMAND)
        .put("chown", FS_SHELL_COMMAND)
        .put("chgrp", FS_SHELL_COMMAND)
        .put("copyFromLocal", FS_SHELL_COMMAND)
        .put("moveFromLocal", FS_SHELL_COMMAND)
        .put("get", FS_SHELL_COMMAND)
        .put("getmerge", FS_SHELL_COMMAND)
        .build();

    private HDFSFileNameCompletor hdfsFileNameCompletor;

    public HadoopREPL(final Configuration configuration) throws ExitSignal {
        try {
            this.configuration = configuration;
            this.fs = FileSystem.get(configuration);
            this.shell = new FsShell(configuration);
            hdfsFileNameCompletor = new HDFSFileNameCompletor(configuration);
            resetCompletors();
        } catch (final IOException ex) {
            throw new ExitSignal(1, ex.getMessage());
        }
    }

    protected void resetCompletors() {
        removeAllCompletors();
        addCompletors(new ArgumentCompletor(Lists.newArrayList(
            new SimpleCompletor(REPL_COMMANDS.keySet().toArray(new String[0])),
            new MultiCompletor(Lists.newArrayList(hdfsFileNameCompletor))
        )));
    }

    @Override
    protected String evaluate(final String input) throws ExitSignal {
        final String[] inputParts = input.split("\\s+", 2);
        final String command = inputParts[0].toLowerCase();
        if (command.isEmpty()) {
            return "";
        } else if (REPL_COMMANDS.containsKey(command)) {
            return REPL_COMMANDS.get(command).execute(command, inputParts.length == 2 ? inputParts[1].split("\\s+") : new String[0]);
        } else {
            return format("Unknown command \"%s\"", command);
        }
    }

    protected static interface CommandFunction {
        String execute(final String command, final String[] args) throws ExitSignal;
    }

    public static void main(final String[] args) {
        int exitCode = 0;

        try {
            if (args.length < 1) {
                throw new ExitSignal(1, "Usage: ./hadoop-repl <path-to-hadoop-core-site-file>");
            }
            final Configuration configuration = new Configuration(true);
            configuration.addResource(new File(args[0]).toURI().toURL());
            new HadoopREPL(configuration).loop("hadoop> ");
        } catch (final ExitSignal ex) {
            System.err.println(ex.getMessage());
            exitCode = ex.getExitCode();
        } catch (final Exception ex) {
            System.err.println(ex);
            exitCode = 1;
        }

        System.exit(exitCode);
    }
}
