/**
 * Copyright (C) 2004 - 2012 Shopzilla, Inc. 
 * All rights reserved. Unauthorized disclosure or distribution is prohibited.
 */

package com.shopzilla.hadoop;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import jline.Completor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author Jeremy Lucas
 * @since 9/11/12
 */
public class HDFSFileNameCompletor implements Completor {

    private final Path root;
    private final FileSystem fs;

    public HDFSFileNameCompletor(final Configuration conf) throws IOException {
        this(conf, new Path("/"));
    }

    public HDFSFileNameCompletor(final Configuration conf, final Path root) throws IOException {
        fs = FileSystem.get(conf);
        this.root = root;
    }

    @Override
    public int complete(final String buffer, final int cursor, final List candidates) {
        try {

            if (buffer == null) {
                return 0;
            }

            final String translated = buffer;

            final Path f = new Path(root, translated);

            final Path dir;

            if (translated.endsWith(File.separator)) {
                dir = f;
            }
            else {
                dir = f.getParent();
            }

            final Path[] entries = (dir == null) ? new Path[0] : listFiles(dir);

            return matchFiles(buffer, translated, entries, candidates);
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
        finally {
            sortFileNames(candidates);
        }
    }

    protected Path[] listFiles(final Path d) throws IOException {
        return Lists.transform(Arrays.asList(fs.listStatus(d)), new Function<FileStatus, Path>() {
            @Override
            public Path apply(final FileStatus fileStatus) {
                return fileStatus.getPath();
            }
        }).toArray(new Path[0]);
    }

    protected void sortFileNames(final List fileNames) {
        Collections.sort(fileNames);
    }

    public int matchFiles(final String buffer, final String translated, final Path[] entries, final List candidates) throws IOException {
        if (entries == null) {
            return -1;
        }

        int matches = 0;

        // first pass: just count the matches
        for (int i = 0; i < entries.length; i++) {
            if (entries[i].toUri().getPath().startsWith(translated)) {
                matches++;
            }
        }

        // green - executable
        // blue - directory
        // red - compressed
        // cyan - symlink
        for (int i = 0; i < entries.length; i++) {
            if (entries[i].toUri().getPath().startsWith(translated)) {
                String name =
                    entries[i].getName()
                        + (((matches == 1) && !fs.isFile(entries[i]))
                        ? File.separator : " ");

                /*
                if (entries [i].isDirectory ())
                {
                        name = new ANSIBuffer ().blue (name).toString ();
                }
                */
                candidates.add(name);
            }
        }

        final int index = buffer.lastIndexOf(File.separator);

        return index + File.separator.length();
    }
}
