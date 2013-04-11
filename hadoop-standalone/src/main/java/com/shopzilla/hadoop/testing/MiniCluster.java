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
    public static final File DEFAULT_CORE_SITE = new File(System.getProperty("user.dir"), "core-site.xml");
    public static final File DEFAULT_MR_LOGS = new File(System.getProperty("user.dir"), "minimrcluster-logs");

    private final File localRoot;
    private final File logDirectory = DEFAULT_MR_LOGS;
    private final Configuration configuration;
    private final File configurationFile;
    private DFSCluster dfsCluster;
    private JobTracker jobTracker;

    public MiniCluster() {
        this(null, DEFAULT_CORE_SITE);
    }

    public MiniCluster(final File configurationFile, final File localRoot) {
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
        if (jobTracker != null) {
            jobTracker.stop();
        }
        if (dfsCluster != null) {
            dfsCluster.stop();
        }
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
