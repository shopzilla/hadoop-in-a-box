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
package com.shopzilla.hadoop.repl;

import com.google.common.base.Joiner;
import com.shopzilla.hadoop.repl.commands.Command;
import com.shopzilla.hadoop.repl.commands.util.ClusterStateManager;
import org.apache.hadoop.conf.Configuration;

import java.io.File;

/**
 * @author Jeremy Lucas
 * @since 4/11/13
 */
public class SessionState {

    protected static final Joiner SPACE_JOINER = Joiner.on(' ').skipNulls();

    public final Configuration configuration;

    private final REPL repl;

    private final ClusterStateManager clusterStateManager;

    public SessionState(final Configuration configuration, final REPL repl) {
        this.configuration = configuration;
        this.repl = repl;
        this.clusterStateManager = new ClusterStateManager(configuration);
    }

    public void saveClusterState(final String outputFileName) {
        clusterStateManager.serialize(new File(outputFileName));
    }

    public void output(final String message, final Object... args) {
        repl.output(message, args);
    }

    public void error(final Throwable t, final Object... args) {
        error(t.getMessage(), args);
    }

    public void error(final String message, final Object... args) {
        repl.error(message, args);
    }

    public Iterable<String> history() {
        return repl.history();
    }

    public void shutdown() {
        repl.shutdown();
    }

    public void outputColumns(final int width, final String... cols) {
        repl.outputColumns(width, cols);
    }

    public void outputColumns(final int width, final Iterable<String> cols) {
        repl.outputColumns(width, cols);
    }

    public void outputUsage(final Command command) {
        outputUsage(command.usage(this));
    }

    public void outputUsage(final Command.Usage usage) {
        outputColumns(1, usage.command, SPACE_JOINER.join(usage.arguments), usage.description);
    }
}
