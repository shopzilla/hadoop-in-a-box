/**
 * Copyright (C) 2004 - 2012 Shopzilla, Inc. 
 * All rights reserved. Unauthorized disclosure or distribution is prohibited.
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
