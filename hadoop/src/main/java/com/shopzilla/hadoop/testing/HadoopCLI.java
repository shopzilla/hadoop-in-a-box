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

/**
 * @author Jeremy Lucas
 * @since 9/5/12
 */
public class HadoopCLI {

    private static final String DEFAULT_CORE_SITE_LOCATION = "/tmp/core-site.xml";
    private static final String DEFAULT_MR_LOGS_LOCATION = "/tmp/minimrcluster/logs";

    private final File localRoot;
    private final File logDirectory = new File(DEFAULT_MR_LOGS_LOCATION);
    private final Configuration configuration;
    private final File configurationFile;
    private DFSCluster dfsCluster;
    private JobTracker jobTracker;

    protected HadoopCLI() {
        this(null, new File(DEFAULT_CORE_SITE_LOCATION));
    }

    protected HadoopCLI(final File localRoot, final File configurationFile) {
        this.configuration = new Configuration();
        this.configurationFile = configurationFile;
        this.localRoot = localRoot;
        System.setProperty("hadoop.log.dir", logDirectory.getAbsolutePath());
    }

    @PostConstruct
    public void start() throws REPL.ExitSignal {
        try {
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
        FileUtils.deleteQuietly(configurationFile);
    }

    public static void main(final String[] args) {
        int exitCode = 0;
        HadoopCLI hadoopCLI = null;
        try {
            File localRoot = null;
            File configurationFile = new File(DEFAULT_CORE_SITE_LOCATION);
            if (args.length >= 1) {
                localRoot = new File(args[0]);
            }
            if (args.length == 2) {
                configurationFile = new File(args[1]);
            }
            if (args.length > 2) {
                throw new REPL.ExitSignal(1, "Usage: ./hdp /path/to/local/hdfs/root [HADOOP_CORE_SITE_FILE]");
            }
            hadoopCLI = new HadoopCLI(localRoot, configurationFile);
            hadoopCLI.start();
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
