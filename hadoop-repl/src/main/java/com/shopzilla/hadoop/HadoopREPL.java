/**
 * Copyright (C) 2004 - 2012 Shopzilla, Inc. 
 * All rights reserved. Unauthorized disclosure or distribution is prohibited.
 */

package com.shopzilla.hadoop;

import jline.ArgumentCompletor;
import jline.Completor;
import jline.SimpleCompletor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;

import java.io.File;
import java.io.IOException;

/**
 * @author Jeremy Lucas
 * @since 9/12/12
 */
public class HadoopREPL extends REPL {

    private final Configuration configuration;
    private FsShell shell;
    private static final String[] HADOOP_COMMANDS = new String[] {
        "cd",
        "pwd",
        "submit",
        "ls",
        "lsr",
        "df",
        "du",
        "dus",
        "count",
        "mv",
        "cp",
        "rm",
        "rmr",
        "expunge",
        "put",
        "copyFromLocal",
        "moveFromLocal",
        "get",
        "getmerge",
        "cat",
        "text",
        "copyToLocal",
        "moveToLocal",
        "mkdir",
        "setrep",
        "touchz",
        "stat",
        "tail",
        "chmod",
        "chown",
        "chgrp",
        "help",
        "quit",
        "exit"
    };

    public HadoopREPL(final Configuration configuration) throws IOException {
        this.configuration = configuration;
        this.shell = new FsShell(configuration);
        addCompletors(new ArgumentCompletor(new Completor[] {
            new SimpleCompletor(
                HADOOP_COMMANDS
            ),
            new HDFSFileNameCompletor(configuration)
        }));
    }

    @Override
    protected String evaluate(final String cmd) throws ExitSignal {
        if (cmd.equalsIgnoreCase("quit") || cmd.equalsIgnoreCase("exit")) {
            throw new ExitSignal(0);
        }
        else {
            runCommand("-" + cmd);
        }
        return "";
    }

    private void runCommand(final String line) {
        try {
            shell.run(line.split(" "));
        }
        catch (final Exception ex) {
            System.err.println(ex);
        }
    }

    public static void main(final String[] args) {
        int exitCode = 0;

        try {
            if (args.length < 1) {
                System.err.println("Usage: ./hadoop-repl /path/to/core-site.xml");
                throw new ExitSignal(1);
            }
            final Configuration configuration = new Configuration(true);
            configuration.addResource(new File(args[0]).toURI().toURL());

            new HadoopREPL(configuration).loop("hadoop> ");
        }
        catch (final ExitSignal ex) {
            exitCode = ex.getExitCode();
        }
        catch (final Exception ex) {
            System.err.println(ex);
            exitCode = 1;
        }

        System.exit(exitCode);
    }
}
