/**
 * Copyright (C) 2004 - 2012 Shopzilla, Inc. 
 * All rights reserved. Unauthorized disclosure or distribution is prohibited.
 */

package com.shopzilla.hadoop.testing;

import com.shopzilla.hadoop.testing.hdfs.DFSCluster;
import com.shopzilla.hadoop.testing.mapreduce.JobTracker;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Scanner;

import static java.lang.String.format;

/**
 * @author Jeremy Lucas
 * @since 9/5/12
 */
public class HadoopCLI {

    private File localRoot;
    private FsShell shell;
    private File logDirectory = new File("/tmp/minimrcluster/logs");
    private Configuration configuration;
    private DFSCluster dfsCluster;
    private JobTracker jobTracker;

    protected HadoopCLI() throws Exception {
        configuration = new Configuration();
        final File confFile = new File("/tmp/hadoop-site.xml");

        configuration.setInt("mapred.submit.replication", 1);
        configuration.set("dfs.datanode.address", "0.0.0.0:0");
        configuration.set("dfs.datanode.http.address", "0.0.0.0:0");
        configuration.writeXml(new FileOutputStream(confFile));

        System.setProperty("hadoop.log.dir", logDirectory.getAbsolutePath());
        System.setProperty("javax.xml.parsers.SAXParserFactory", "com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");
    }

    @PostConstruct
    public void start() {
        dfsCluster = new DFSCluster();
        dfsCluster.setLocalRoot(localRoot);
        dfsCluster.setConfiguration(configuration);
        dfsCluster.start();

        jobTracker = new JobTracker();
        jobTracker.setDfsNameNode(dfsCluster.getFileSystem().getUri().toString());
        jobTracker.start();

        System.out.println(format("[HDFS HTTP: %s]", configuration.get("dfs.http.address")));
        System.out.println(format("[JobTracker HTTP: %s]", configuration.get("mapred.job.tracker.http.address")));

        shell = new FsShell(configuration);
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

    public static void main(final String[] args) throws Exception {

        if (args.length != 1) {
            System.err.println("Usage: ./hdp /path/to/local/hdfs/root");
            System.exit(1);
        }

        final Scanner scanner = new Scanner(System.in);

        final HadoopCLI hadoopCLI = new HadoopCLI();
        hadoopCLI.setLocalRoot(new File(args[0]));
        final File logDirectory = new File("/tmp/minimrcluster/logs");
        hadoopCLI.setLogDirectory(logDirectory);

        hadoopCLI.start();

        String cmd;

        while (true) {
            System.out.print("\nhdfs> ");
            cmd = scanner.nextLine();
            if (cmd.equals("exit") || cmd.equals("quit")) {
                break;
            } else {
                hadoopCLI.runCommand("-" + cmd);
            }
        }

        hadoopCLI.stop();
    }
}
