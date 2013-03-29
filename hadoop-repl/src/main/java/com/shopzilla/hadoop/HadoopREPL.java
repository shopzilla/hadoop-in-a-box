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
import jline.ArgumentCompletor;
import jline.Completor;
import jline.SimpleCompletor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static java.lang.String.format;

/**
 * @author Jeremy Lucas
 * @since 9/12/12
 */
public class HadoopREPL extends REPL {

    private Path currentWorkingDirectory = new Path("/");

    private final Configuration configuration;

    private final FileSystem fs;

    private final FsShell shell;

    private final CommandFunction FS_SHELL_COMMAND = new CommandFunction() {
        @Override
        public String execute(final String command, final String[] args) {
            try {
                shell.run(Joiner.on(" ").join("-" + command, Joiner.on(" ").join(args)).split(" "));
            }
            catch (final Exception ex) {
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
//        .put("cd", new CommandFunction() {
//            @Override
//            public String execute(final String command, final String[] args) throws ExitSignal {
//                final String newDirectory = args[0];
//                Path newPath;
//                if (newDirectory.startsWith(File.separator)) {
//                    newPath = new Path(newDirectory);
//                } else {
//                    newPath = resolvePath(currentWorkingDirectory, newDirectory);
//                }
//
//                try {
//                    if (fs.exists(newPath) && !fs.isFile(newPath)) {
//                        currentWorkingDirectory = newPath;
//                        hdfsFileNameCompletor = new HDFSFileNameCompletor(configuration, currentWorkingDirectory);
//                        resetCompletors();
//                    } else if (fs.isFile(newPath)) {
//                        System.err.println(format("Must specify a directory [%s]", newPath));
//                    } else {
//                        System.err.println(format("No such directory [%s]", newPath));
//                    }
//                } catch (final IOException ex) {
//                    System.err.println(ex);
//                }
//                return null;
//            }
//        })
//        .put("pwd", new CommandFunction() {
//            @Override
//            public String execute(final String command, final String[] args) throws ExitSignal {
//                return currentWorkingDirectory.toString();
//            }
//        })
//        .put("submit", FS_SHELL_COMMAND)
        .put("help", new CommandFunction() {
            @Override
            public String execute(final String command, final String[] args) throws ExitSignal {
                if (args.length != 1) {
                    return "Usage: help [command]";
                } else {
                    if (REPL_COMMANDS.containsKey(args[0])) {
                        return format("Displaying help for \"%s\":\n", args[0]);
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
        .put("copyFromLocal", FS_SHELL_COMMAND)
        .put("moveFromLocal", FS_SHELL_COMMAND)
        .put("get", FS_SHELL_COMMAND)
        .put("getmerge", FS_SHELL_COMMAND)
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
        addCompletors(new ArgumentCompletor(new Completor[] {
            new SimpleCompletor(REPL_COMMANDS.keySet().toArray(new String[0])),
            hdfsFileNameCompletor
        }));
    }

    protected Path resolvePath(final Path currentPath, final String newPath) {
        final String[] pathParts = newPath.split(File.separator);
        if (pathParts[0].startsWith("..")) {
            return resolvePath(currentPath.getParent(), Joiner.on(File.separator).join(Arrays.copyOfRange(pathParts, 1, pathParts.length)));
        } else {
            return new Path(currentPath, newPath);
        }
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
                throw new ExitSignal(1, "Usage: ./hadoop-repl /path/to/core-site.xml");
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
