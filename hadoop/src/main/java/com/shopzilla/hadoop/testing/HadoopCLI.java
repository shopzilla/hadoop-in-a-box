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

import com.shopzilla.hadoop.HadoopREPL;
import com.shopzilla.hadoop.REPL;
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

    protected HadoopCLI() {
        configuration = new Configuration();

        System.setProperty("hadoop.log.dir", logDirectory.getAbsolutePath());
    }

    @PostConstruct
    public void start(final File confFile) throws REPL.ExitSignal {
        try {
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
            new HadoopREPL(configuration).loop("hadoop-in-a-box> ");
        } catch (final IOException ex) {
            throw new REPL.ExitSignal(1, ex.getMessage());
        }
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
        } catch (final REPL.ExitSignal ex) {
            exitCode = ex.getExitCode();
            if (exitCode == 0) {
                System.out.println(ex.getMessage());
            } else {
                System.err.println(ex.getMessage());
            }
        } finally {
            if (hadoopCLI != null) {
                hadoopCLI.stop();
            }
        }
        System.exit(exitCode);
    }
}
