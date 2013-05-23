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

package com.shopzilla.hadoop.testing.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.MiniMRClientCluster;
import org.apache.hadoop.mapred.MiniMRClientClusterFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;

/**
 * @author Jeremy Lucas
 * @since 9/5/12
 */
public class JobTracker {

    private final Configuration configuration;
    private MiniMRClientCluster miniMrCluster;

    public static class Builder {
        private Configuration configuration;

        public Builder usingConfiguration(final Configuration configuration) {
            this.configuration = configuration;
            return this;
        }

        public JobTracker build() {
            return new JobTracker(configuration);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public JobTracker(final Configuration configuration) {
        this.configuration = configuration;
    }

    @PostConstruct
    public JobTracker start() {
        try {
            MiniMRClientClusterFactory.create(JobTracker.class, 4, configuration);
            miniMrCluster = MiniMRClientClusterFactory.create(JobTracker.class, 4, configuration);
            miniMrCluster.start();
            return this;
        } catch (final IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public String getHttpAddress() {
        return "";
    }

    @PreDestroy
    public void stop() {
        try {
            final Thread shutdownThread = new Thread(new Runnable() {

                @Override
                public void run() {
                    try {
                        if (miniMrCluster != null) {
                            miniMrCluster.stop();
                            miniMrCluster = null;
                        }
                    } catch (final Exception ex) {
                        ex.printStackTrace();
                    }
                }
            });
            shutdownThread.start();
            shutdownThread.join(10000);
        }
        catch (final InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }
}
