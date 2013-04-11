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

import com.shopzilla.hadoop.repl.HadoopREPL;
import com.shopzilla.hadoop.repl.REPL;

import java.io.File;
import java.io.IOException;

/**
 * @author Jeremy Lucas
 * @since 9/5/12
 */
public class HadoopStandaloneCLI {

    public static void main(final String[] args) {
        int exitCode = 0;
        try {
            File localRoot = null;
            File configurationFile = MiniCluster.DEFAULT_CORE_SITE;
            if (args.length >= 1) {
                configurationFile = new File(args[0]);
            }
            if (args.length == 2) {
                localRoot = new File(args[1]);
            }
            if (args.length > 2) {
                throw new REPL.ExitSignal(1, "Usage: ./hadoop-standalone [<path-to-hadoop-core-site-file>] [<local-root-directory>]");
            }
            final MiniCluster miniCluster = new MiniCluster(configurationFile, localRoot);
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    miniCluster.stop();
                }
            }));
            miniCluster.start();
            System.out.println("DFS HTTP: " + miniCluster.getDfsCluster().getHttpAddress());
            System.out.println("JobTracker HTTP: " + miniCluster.getJobTracker().getHttpAddress());
            new HadoopREPL(miniCluster.getConfiguration()).loop("hadoop-in-a-box> ");
        } catch (final IOException ex) {
            exitCode = 100;
            System.err.println(ex.getMessage());
        } catch (final REPL.ExitSignal ex) {
            exitCode = ex.getExitCode();
            if (exitCode == 0) {
                System.out.println(ex.getMessage());
            } else {
                System.err.println(ex.getMessage());
            }
        }
        System.exit(exitCode);
    }
}
