/**
 * Copyright (C) 2004 - 2012 Shopzilla, Inc. 
 * All rights reserved. Unauthorized disclosure or distribution is prohibited.
 */

package com.shopzilla.hadoop.testing.hdfs;

import com.google.common.base.Function;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @author Jeremy Lucas
 * @since 9/5/12
 */
public class DFSCluster {

    private Configuration configuration;
    private MiniDFSCluster miniDFSCluster;
    private Path hdfsRoot;
    private File localRoot;
    private File projectDirectory;
    private File buildDirectory;
    /*private TServer hiveServer;
  private HiveClient hiveClient;*/

    @PostConstruct
    public void start() {
        try {
            this.hdfsRoot = new Path(localRoot.getName());
            miniDFSCluster = new MiniDFSCluster(configuration, 2, true, null);

            /*hiveServer = createHiveServer();
          new Thread(new Runnable() {
              @Override
              public void run() {
                  hiveServer.serve();
              }
          }).start();

          hiveClient = createHiveClient();*/

            buildDirectory = new File(miniDFSCluster.getDataDirectory()).getParentFile().getParentFile().getParentFile().getParentFile();
            projectDirectory = buildDirectory.getParentFile();

            importHDFSDirectory(localRoot);
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void importHDFSDirectory(final File file) throws Exception {
        Path path = new Path(hdfsRoot, "/" + localRoot.toURI().relativize(file.toURI()).getPath());
        if (file.isDirectory()) {
            getFileSystem().mkdirs(path);
            getFileSystem().makeQualified(path);
            for (File child : file.listFiles()) {
                importHDFSDirectory(child);
            }
        }
        else {
            getFileSystem().copyFromLocalFile(false, true, new Path(file.getAbsolutePath()), path);
            getFileSystem().makeQualified(path);
        }
    }

    /*
    private TServer createHiveServer(String...args) {
        try {
            HiveServer.HiveServerCli cli = new HiveServer.HiveServerCli();
            cli.parse(args);

            Properties hiveconf = cli.addHiveconfToSystemProperties();
            hiveconf.putAll(ConfigurationUtil.toProperties(configuration));

            HiveConf conf = new HiveConf(HiveServer.HiveServerHandler.class);
            ServerUtils.cleanUpScratchDir(conf);
            TServerTransport serverTransport = new TServerSocket(cli.port);

            for (Map.Entry item : hiveconf.entrySet()) {
                conf.set((String) item.getKey(), (String) item.getValue());
            }

            HiveServer.ThriftHiveProcessorFactory hfactory = new HiveServer.ThriftHiveProcessorFactory(null, conf);

            TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(serverTransport)
                .processorFactory(hfactory)
                .transportFactory(new TTransportFactory())
                .protocolFactory(new TBinaryProtocol.Factory())
                .minWorkerThreads(cli.minWorkerThreads)
                .maxWorkerThreads(cli.maxWorkerThreads);

            return new TThreadPoolServer(sargs);
        }
        catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private HiveClient createHiveClient() {
        try {
            final TTransport transport = new TSocket("0.0.0.0", 10000);
            HiveClient client = new org.apache.hadoop.hive.service.HiveClient(new TBinaryProtocol(transport));
            transport.open();
            return client;
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }*/

    public FileSystem getFileSystem() {
        try {
            return miniDFSCluster.getFileSystem();
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    /*
    public HiveClient getHiveClient() {
        return hiveClient;
    }
    */


    public void processPaths(final Path path, final Function<Path, Void> pathProcessor) throws IOException {
        if (miniDFSCluster.getFileSystem().exists(path)) {
            FileStatus[] fileStatuses = miniDFSCluster.getFileSystem().listStatus(path);
            for (FileStatus fileStatus : fileStatuses) {
                if (!fileStatus.getPath().toUri().getPath().startsWith("_")) {
                    pathProcessor.apply(fileStatus.getPath());
                }
            }
        }
        else {
            throw new IOException("Path does not exist: " + path);
        }
    }

    public void processPathsRecursive(final Path path, final Function<Path, Void> pathProcessor) throws IOException {
        if (miniDFSCluster.getFileSystem().exists(path)) {
            if (miniDFSCluster.getFileSystem().isFile(path)) {
                if (!path.toUri().getPath().startsWith("_")) {
                    pathProcessor.apply(path);
                }
            }
            else {
                FileStatus[] fileStatuses = miniDFSCluster.getFileSystem().listStatus(path);
                for (FileStatus fileStatus : fileStatuses) {
                    if (!fileStatus.getPath().toUri().getPath().startsWith("_")) {
                        processPathsRecursive(fileStatus.getPath(), pathProcessor);
                    }
                }
            }
        }
        else {
            throw new IOException("Path does not exist: " + path);
        }
    }

    public void processData(final Path path, final Function<String, Void> lineProcessor) throws IOException {
        final Function<Path, Void> pathProcessor = new Function<Path, Void>() {
            @Override
            public Void apply(Path path) {
                try {
                    FSDataInputStream in = miniDFSCluster.getFileSystem().open(path);
                    LineIterator lineIterator = new LineIterator(new InputStreamReader(in));
                    while (lineIterator.hasNext()) {
                        lineProcessor.apply(lineIterator.next());
                    }
                    lineIterator.close();
                }
                catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
                return null;
            }
        };
        processPaths(path, new Function<Path, Void>() {
            @Override
            public Void apply(Path input) {
                pathProcessor.apply(input);
                return null;
            }
        });
    }

    public void processDataRecursive(final Path path, final Function<String, Void> lineProcessor) throws IOException {
        final Function<Path, Void> pathProcessor = new Function<Path, Void>() {
            @Override
            public Void apply(Path path) {
                try {
                    FSDataInputStream in = miniDFSCluster.getFileSystem().open(path);
                    LineIterator lineIterator = new LineIterator(new InputStreamReader(in));
                    while (lineIterator.hasNext()) {
                        lineProcessor.apply(lineIterator.next());
                    }
                    lineIterator.close();
                }
                catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
                return null;
            }
        };
        processPathsRecursive(path, new Function<Path, Void>() {
            @Override
            public Void apply(Path input) {
                pathProcessor.apply(input);
                return null;
            }
        });
    }

    @PreDestroy
    public void stop() {
        try {
            Thread shutdownThread = new Thread(new Runnable() {

                @Override
                public void run() {
                    try {
                        /*
                        if (hiveServer != null) {
                            hiveServer.stop();
                        }
                        */
                        if (miniDFSCluster != null) {
                            miniDFSCluster.shutdown();
                            miniDFSCluster = null;
                        }
                    }
                    catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            });
            shutdownThread.start();
            shutdownThread.join(10000);
            FileUtils.deleteDirectory(buildDirectory);
            FileUtils.deleteDirectory(new File(projectDirectory, "logs"));
            //FileUtils.deleteDirectory(new File(projectDirectory, "metastore_db"));
            //FileUtils.deleteQuietly(new File(projectDirectory, "derby.log"));
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void setConfiguration(final Configuration configuration) {
        this.configuration = configuration;
    }

    public void setLocalRoot(final File localRoot) {
        this.localRoot = localRoot;
    }

}
