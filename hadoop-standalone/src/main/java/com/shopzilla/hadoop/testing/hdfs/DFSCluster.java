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

    private final Configuration configuration;
    private final File localRoot;
    private final int numberOfDataNodes;

    private MiniDFSCluster miniDFSCluster;
    private File projectDirectory;
    private File buildDirectory;

    public static class Builder {
        private Configuration configuration = new Configuration();
        private File localRoot;
        private int numberOfDataNodes = 4;

        public Builder usingConfiguration(final Configuration configuration) {
            this.configuration = configuration;
            return this;
        }

        public Builder withDataNodes(final int numberOfDataNodes) {
            this.numberOfDataNodes = numberOfDataNodes;
            return this;
        }

        public Builder withInitialStructure(final File localRoot) {
            this.localRoot = localRoot;
            return this;
        }

        public DFSCluster build() {
            return new DFSCluster(configuration, localRoot, numberOfDataNodes);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public DFSCluster(final Configuration configuration, final File localRoot, final int numberOfDataNodes) {
        this.configuration = configuration;
        this.localRoot = localRoot;
        this.numberOfDataNodes = numberOfDataNodes;
    }

    @PostConstruct
    public DFSCluster start() {
        try {
            miniDFSCluster = new MiniDFSCluster(configuration, numberOfDataNodes, true, null);
            buildDirectory = new File(System.getProperty("user.dir"), "build");
            projectDirectory = buildDirectory.getParentFile();
            if (localRoot != null) {
                importHDFSDirectory(new Path(localRoot.getName()), localRoot);
            }
            return this;
        } catch (final IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void importHDFSDirectory(final Path hdfsRoot, final File file) throws IOException {
        final Path path = new Path(hdfsRoot, File.separator + localRoot.toURI().relativize(file.toURI()).getPath());
        if (file.isDirectory()) {
            getFileSystem().mkdirs(path);
            getFileSystem().makeQualified(path);
            for (final File child : file.listFiles()) {
                importHDFSDirectory(hdfsRoot, child);
            }
        } else {
            getFileSystem().copyFromLocalFile(false, true, new Path(file.getAbsolutePath()), path);
            getFileSystem().makeQualified(path);
        }
    }

    public String getHttpAddress() {
        return "http://localhost:" + miniDFSCluster.getNameNode().getHttpAddress().getPort();
    }

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

    public MiniDFSCluster getMiniDFSCluster() {
        return miniDFSCluster;
    }

    public void processPaths(final Path path, final Function<Path, Void> pathProcessor) throws IOException {
        if (miniDFSCluster.getFileSystem().exists(path)) {
            final FileStatus[] fileStatuses = miniDFSCluster.getFileSystem().listStatus(path);
            for (final FileStatus fileStatus : fileStatuses) {
                if (!fileStatus.getPath().toUri().getPath().startsWith("_")) {
                    pathProcessor.apply(fileStatus.getPath());
                }
            }
        } else {
            throw new IOException("Path does not exist: " + path);
        }
    }

    public void processPathsRecursive(final Path path, final Function<Path, Void> pathProcessor) throws IOException {
        if (miniDFSCluster.getFileSystem().exists(path)) {
            if (miniDFSCluster.getFileSystem().isFile(path)) {
                if (!path.toUri().getPath().startsWith("_")) {
                    pathProcessor.apply(path);
                }
            } else {
                final FileStatus[] fileStatuses = miniDFSCluster.getFileSystem().listStatus(path);
                for (final FileStatus fileStatus : fileStatuses) {
                    if (!fileStatus.getPath().toUri().getPath().startsWith("_")) {
                        processPathsRecursive(fileStatus.getPath(), pathProcessor);
                    }
                }
            }
        } else {
            throw new IOException("Path does not exist: " + path);
        }
    }

    public void processData(final Path path, final Function<String, Void> lineProcessor) throws IOException {
        final Function<Path, Void> pathProcessor = new Function<Path, Void>() {
            @Override
            public Void apply(Path path) {
                try {
                    final FSDataInputStream in = miniDFSCluster.getFileSystem().open(path);
                    final LineIterator lineIterator = new LineIterator(new InputStreamReader(in));
                    while (lineIterator.hasNext()) {
                        lineProcessor.apply(lineIterator.next());
                    }
                    lineIterator.close();
                    return null;
                } catch (final Exception ex) {
                    throw new RuntimeException(ex);
                }
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
                    final FSDataInputStream in = miniDFSCluster.getFileSystem().open(path);
                    final LineIterator lineIterator = new LineIterator(new InputStreamReader(in));
                    while (lineIterator.hasNext()) {
                        lineProcessor.apply(lineIterator.next());
                    }
                    lineIterator.close();
                    return null;
                } catch (final Exception ex) {
                    throw new RuntimeException(ex);
                }
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
            final Thread shutdownThread = new Thread(new Runnable() {

                @Override
                public void run() {
                    if (miniDFSCluster != null) {
                        miniDFSCluster.shutdown();
                        miniDFSCluster = null;
                    }
                }
            });
            shutdownThread.start();
            shutdownThread.join(10000);
            FileUtils.deleteQuietly(buildDirectory);
            FileUtils.deleteQuietly(new File(projectDirectory, "logs"));
        } catch (final InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }
}
