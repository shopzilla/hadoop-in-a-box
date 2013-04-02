/**
 * Copyright (C) 2004 - 2013 Shopzilla, Inc. 
 * All rights reserved. Unauthorized disclosure or distribution is prohibited.
 */
package com.shopzilla.hadoop.testing;

import com.shopzilla.hadoop.testing.hdfs.DFSCluster;
import com.shopzilla.hadoop.testing.mapreduce.JobTracker;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author jlucas
 * @since 4/1/13
 */
public class MiniCluster {
    private static final String DEFAULT_CORE_SITE_LOCATION = "/tmp/core-site.xml";
    private static final String DEFAULT_MR_LOGS_LOCATION = "/tmp/minimrcluster/logs";

    private final File localRoot;
    private final File logDirectory = new File(DEFAULT_MR_LOGS_LOCATION);
    private final Configuration configuration;
    private final File configurationFile;
    private DFSCluster dfsCluster;
    private JobTracker jobTracker;

    public MiniCluster() {
        this(null, new File(DEFAULT_CORE_SITE_LOCATION));
    }

    public MiniCluster(final File localRoot, final File configurationFile) {
        this.configuration = new Configuration();
        this.configurationFile = configurationFile;
        this.localRoot = localRoot;
        System.setProperty("hadoop.log.dir", logDirectory.getAbsolutePath());
    }

    @PostConstruct
    public void start() throws IOException {
        dfsCluster = DFSCluster.builder()
            .usingConfiguration(configuration)
            .withInitialStructure(localRoot)
            .build()
            .start();

        jobTracker = JobTracker.builder()
            .withNameNode(dfsCluster.getFileSystem().getUri().toString())
            .build()
            .start();

        configuration.writeXml(new FileOutputStream(configurationFile));
    }

    @PreDestroy
    public void stop() {
        jobTracker.stop();
        dfsCluster.stop();
        FileUtils.deleteQuietly(logDirectory);
        FileUtils.deleteQuietly(configurationFile);
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public DFSCluster getDfsCluster() {
        return dfsCluster;
    }

    public JobTracker getJobTracker() {
        return jobTracker;
    }
}
