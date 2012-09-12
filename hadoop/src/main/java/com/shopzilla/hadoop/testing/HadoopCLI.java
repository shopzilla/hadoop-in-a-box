/**
 * Copyright (C) 2004 - 2012 Shopzilla, Inc. 
 * All rights reserved. Unauthorized disclosure or distribution is prohibited.
 */

package com.shopzilla.hadoop.testing;

import com.shopzilla.common.io.console.REPL;
import com.shopzilla.hadoop.testing.hdfs.DFSCluster;
import com.shopzilla.hadoop.testing.hdfs.HDFSFileNameCompletor;
import com.shopzilla.hadoop.testing.mapreduce.JobTracker;
import jline.ArgumentCompletor;
import jline.Completor;
import jline.SimpleCompletor;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import static java.lang.String.format;

/**
 * @author Jeremy Lucas
 * @since 9/5/12
 */
public class HadoopCLI extends REPL {

    private File localRoot;
    private FsShell shell;
    private File logDirectory = new File("/tmp/minimrcluster/logs");
    private Configuration configuration;
    private DFSCluster dfsCluster;
    private JobTracker jobTracker;
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

    protected HadoopCLI() throws IOException {
        configuration = new Configuration();

        configuration.setInt("mapred.submit.replication", 1);
        configuration.set("dfs.datanode.address", "0.0.0.0:0");
        configuration.set("dfs.datanode.http.address", "0.0.0.0:0");

        System.setProperty("hadoop.log.dir", logDirectory.getAbsolutePath());
        System.setProperty("javax.xml.parsers.SAXParserFactory", "com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");
    }

    @PostConstruct
    public void start() throws IOException {
        dfsCluster = new DFSCluster();
        dfsCluster.setLocalRoot(localRoot);
        dfsCluster.setConfiguration(configuration);
        dfsCluster.start();

        jobTracker = new JobTracker();
        jobTracker.setDfsNameNode(dfsCluster.getFileSystem().getUri().toString());
        jobTracker.start();

        System.out.println(format("[HDFS HTTP: %s]", configuration.get("dfs.http.address")));
        System.out.println(format("[JobTracker HTTP: localhost:%s]", jobTracker.getJobTrackerRunner().getJobTrackerInfoPort()));

        addCompletors(new ArgumentCompletor(new Completor[] {
            new SimpleCompletor(
                HADOOP_COMMANDS
            ),
            new HDFSFileNameCompletor(configuration)
        }));

        shell = new FsShell(configuration);

        final File confFile = new File("/tmp/core-site.xml");
        configuration.writeXml(new FileOutputStream(confFile));
    }

    @PreDestroy
    public void stop() {
        jobTracker.stop();
        dfsCluster.stop();
        FileUtils.deleteQuietly(logDirectory);
    }

    public void runCommand(final String line) {
        try {
            shell.run(line.split(" "));
        } catch (final Exception ex) {
            System.err.println(ex);
        }
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setLocalRoot(final File localRoot) {
        this.localRoot = localRoot;
    }

    public void setLogDirectory(final File logDirectory) {
        this.logDirectory = logDirectory;
    }

    public static void main(final String[] args) {
        int exitCode = 0;
        HadoopCLI hadoopCLI = null;
        try {
            if (args.length < 1) {
                System.err.println("Usage: ./hdp /path/to/local/hdfs/root");
                throw new ExitSignal(1);
            }
            hadoopCLI = new HadoopCLI();
            hadoopCLI.setLocalRoot(new File(args[0]));
            final File logDirectory = new File("/tmp/minimrcluster/logs");
            hadoopCLI.setLogDirectory(logDirectory);
            hadoopCLI.start();
            hadoopCLI.loop("hadoop> ");
        } catch (final ExitSignal ex) {
            exitCode = ex.getExitCode();
        } catch (final Exception ex) {
            System.err.println(ex);
            exitCode = 1;
        } finally {
            if (hadoopCLI != null) {
                hadoopCLI.stop();
            }
            System.exit(exitCode);
        }
    }

    @Override
    protected String evaluate(final String cmd) throws ExitSignal {
        if (cmd.equalsIgnoreCase("quit") || cmd.equalsIgnoreCase("exit")) {
            throw new ExitSignal(0);
        } else {
            runCommand("-" + cmd);
        }
        return "";
    }
}
