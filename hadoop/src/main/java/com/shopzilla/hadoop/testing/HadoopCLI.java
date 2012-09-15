/**
 * Copyright (C) 2004 - 2012 Shopzilla, Inc. 
 * All rights reserved. Unauthorized disclosure or distribution is prohibited.
 */

package com.shopzilla.hadoop.testing;

import com.shopzilla.hadoop.HadoopREPL;
import com.shopzilla.hadoop.testing.hdfs.DFSCluster;
import com.shopzilla.hadoop.testing.mapreduce.JobTracker;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

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
public class HadoopCLI {

    private static final String DEFAULT_CORE_SITE_LOCATION = "/tmp/core-site.xml";
    private static final String DEFAULT_MR_LOGS_LOCATION = "/tmp/minimrcluster/logs";

    private File localRoot;
    private File logDirectory = new File(DEFAULT_MR_LOGS_LOCATION);
    private Configuration configuration;
    private DFSCluster dfsCluster;
    private JobTracker jobTracker;

    protected HadoopCLI() throws IOException {
        configuration = new Configuration();

        configuration.setInt("mapred.submit.replication", 1);
        configuration.set("dfs.datanode.address", "0.0.0.0:0");
        configuration.set("dfs.datanode.http.address", "0.0.0.0:0");

        System.setProperty("hadoop.log.dir", logDirectory.getAbsolutePath());
        System.setProperty("javax.xml.parsers.SAXParserFactory", "com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");
    }

    @PostConstruct
    public void start(final File confFile) throws IOException {
        dfsCluster = new DFSCluster();
        dfsCluster.setLocalRoot(localRoot);
        dfsCluster.setConfiguration(configuration);
        dfsCluster.start();

        jobTracker = new JobTracker();
        jobTracker.setDfsNameNode(dfsCluster.getFileSystem().getUri().toString());
        jobTracker.start();

        System.out.println(format("[HDFS HTTP: %s]", configuration.get("dfs.http.address")));
        System.out.println(format("[M/R HTTP : %s]", jobTracker.getJobTrackerRunner().getJobTrackerInfoPort()));

        configuration.writeXml(new FileOutputStream(confFile));
        HadoopREPL.main(new String[] { confFile.getAbsolutePath() });
    }

    @PreDestroy
    public void stop() {
        jobTracker.stop();
        dfsCluster.stop();
        FileUtils.deleteQuietly(logDirectory);
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
            if (args.length < 1 || args.length > 2) {
                System.err.println("Usage: ./hdp /path/to/local/hdfs/root [HADOOP_CORE_SITE_FILE]");
                System.exit(1);
            }
            hadoopCLI = new HadoopCLI();
            hadoopCLI.setLocalRoot(new File(args[0]));
            hadoopCLI.start(new File(args.length == 2 ? args[1] : DEFAULT_CORE_SITE_LOCATION));
        } catch (final Throwable t) {
            System.err.println(t);
            exitCode = 1;
        } finally {
            if (hadoopCLI != null) {
                hadoopCLI.stop();
            }
        }
        System.exit(exitCode);
    }
}
