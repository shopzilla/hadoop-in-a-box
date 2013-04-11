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

package com.shopzilla.hadoop.repl.commands.util;

import com.google.common.io.Files;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.zip.GZIPOutputStream;

/**
 * @author Jeremy Lucas
 * @since 4/11/13
 */
public class ClusterStateManager {

    protected final FileSystem fs;

    public ClusterStateManager(final Configuration configuration) {
        try {
            this.fs = FileSystem.get(configuration);
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public ClusterStateManager(final FileSystem fs) {
        this.fs = fs;
    }

    public void serialize(final File output) {
        try {
            serializePath(new Path("/"), output);
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void load(final File archive) {
        try {
            throw new UnsupportedOperationException("Not yet!");
//            TODO: Untargz the archive, then load in!
//            fs.delete(new Path("/"), true);
//            importHDFSDirectory(new Path("/"), archive, archive);
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void importHDFSDirectory(final Path hdfsRoot, final File localRoot, final File file) throws IOException {
        final Path path = new Path(hdfsRoot, File.separator + localRoot.toURI().relativize(file.toURI()).getPath());
        if (file.isDirectory()) {
            fs.mkdirs(path);
            fs.makeQualified(path);
            for (final File child : file.listFiles()) {
                importHDFSDirectory(hdfsRoot, localRoot, child);
            }
        }
        else {
            fs.copyFromLocalFile(false, true, new Path(file.getAbsolutePath()), path);
            fs.makeQualified(path);
        }
    }

    public void serializePath(final Path path, final File output) throws Exception {
        final File tmpRoot = Files.createTempDir();
        final File tmp = new File(tmpRoot, "hdfs");
        FileUtils.forceMkdir(tmp);
        new FsShell(fs.getConf()).run(new String[]{
            "-copyToLocal",
            path.toString(),
            tmp.getAbsolutePath()
        });
        compressFile(tmp, output);
        FileUtils.deleteQuietly(tmpRoot);
    }

    public static void compressFile(File file, File output)
        throws IOException {
        ArrayList<File> list = new ArrayList<File>(1);
        list.add(file);
        compressFiles(list, output);
    }

    public static void compressFiles(Collection<File> files, File output)
        throws IOException {
        // Create the output stream for the output file
        FileOutputStream fos = new FileOutputStream(output);
        // Wrap the output file stream in streams that will tar and gzip everything
        TarArchiveOutputStream taos = new TarArchiveOutputStream(
            new GZIPOutputStream(new BufferedOutputStream(fos)));
        // TAR has an 8 gig file limit by default, this gets around that
        taos.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_STAR); // to get past the 8 gig limit
        // TAR originally didn't support long file names, so enable the support for it
        taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);

        // Get to putting all the files in the compressed output file
        for (File f : files) {
            addFilesToCompression(taos, f, ".");
        }

        // Close everything up
        taos.close();
        fos.close();
    }

    private static void addFilesToCompression(TarArchiveOutputStream taos, File file, String dir)
        throws IOException {
        // Create an entry for the file
        taos.putArchiveEntry(new TarArchiveEntry(file, dir + File.separator + file.getName()));
        if (file.isFile()) {
            // Add the file to the archive
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
            IOUtils.copy(bis, taos);
            taos.closeArchiveEntry();
            bis.close();
        }
        else if (file.isDirectory()) {
            // close the archive entry
            taos.closeArchiveEntry();
            // go through all the files in the directory and using recursion, add them to the archive
            for (File childFile : file.listFiles()) {
                addFilesToCompression(taos, childFile, dir + File.separator + file.getName());
            }
        }
    }

//    protected void compressFile(final Path path, File output)
//        throws IOException {
//        ArrayList<Path> list = new ArrayList<Path>(1);
//        list.add(path);
//        compressFiles(list, output);
//    }
//
//    protected void compressFiles(final Collection<Path> paths, final File output)
//        throws IOException {
//        // Create the output stream for the output file
//        FileOutputStream fos = new FileOutputStream(output);
//        // Wrap the output file stream in streams that will tar and gzip everything
//        TarArchiveOutputStream taos = new TarArchiveOutputStream(
//            new GZIPOutputStream(new BufferedOutputStream(fos)));
//        // TAR has an 8 gig file limit by default, this gets around that
//        taos.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_STAR); // to get past the 8 gig limit
//        // TAR originally didn't support long file names, so enable the support for it
//        taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
//
//        // Get to putting all the files in the compressed output file
//        for (final Path path : paths) {
//            addPathsToCompression(taos, path, ".");
//        }
//
//        // Close everything up
//        taos.close();
//        fos.close();
//    }
//
//    protected void addPathsToCompression(final TarArchiveOutputStream taos, final Path path, final String dir)
//        throws IOException {
//        // Create an entry for the file
//        taos.putArchiveEntry(new TarArchiveEntry(dir + "/" + path.getName()));
//        if (fs.isFile(path)) {
//            // Add the file to the archive
//            BufferedInputStream bis = new BufferedInputStream(fs.open(path));
//            IOUtils.copy(bis, taos);
//            taos.closeArchiveEntry();
//            bis.close();
//        } else {
//            // close the archive entry
//            taos.closeArchiveEntry();
//            // go through all the files in the directory and using recursion, add them to the archive
//            for (FileStatus childFile : fs.listStatus(path)) {
//                addPathsToCompression(taos, childFile.getPath(), path.getName());
//            }
//        }
//    }
}
