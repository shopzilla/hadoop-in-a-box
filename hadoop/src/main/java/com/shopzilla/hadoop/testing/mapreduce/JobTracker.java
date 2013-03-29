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

import org.apache.hadoop.mapred.MiniMRCluster;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * @author Jeremy Lucas
 * @since 9/5/12
 */
public class JobTracker {

    private String dfsNameNode;
    private MiniMRCluster miniMrCluster;
    private int numTaskTrackers = 4;

    @PostConstruct
    public void start() {
        try {
            miniMrCluster = new MiniMRCluster(numTaskTrackers, dfsNameNode, 1);

        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public MiniMRCluster.JobTrackerRunner getJobTrackerRunner() {
        return miniMrCluster.getJobTrackerRunner();
    }

    @PreDestroy
    public void stop() {
        try {
            Thread shutdownThread = new Thread(new Runnable() {

                @Override
                public void run() {
                    try {
                        if (miniMrCluster != null) {
                            miniMrCluster.shutdown();
                            miniMrCluster = null;
                        }
                    }
                    catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            });
            shutdownThread.start();
            shutdownThread.join(10000);
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void setDfsNameNode(final String dfsNameNode) {
        this.dfsNameNode = dfsNameNode;
    }

    public void setNumTaskTrackers(final int numTaskTrackers) {
        this.numTaskTrackers = numTaskTrackers;
    }
}
