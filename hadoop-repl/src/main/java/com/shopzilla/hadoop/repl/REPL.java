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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import jline.console.ConsoleReader;
import jline.console.completer.Completer;
import jline.console.history.History;

import java.io.IOException;
import java.util.LinkedList;

import static java.lang.String.format;

/**
 * @author Jeremy Lucas
 * @since 9/11/12
 */
public abstract class REPL {

    public static final Joiner SPACE_JOINER = Joiner.on(' ').skipNulls();

    protected static final String  DEFAULT_PROMPT = "> ";

    protected boolean shutdown = false;

    protected ConsoleReader consoleReader;

    public REPL() throws ExitSignal {
        try {
            consoleReader = new ConsoleReader();
        } catch (final Exception ex) {
            throw new ExitSignal(1, ex.getMessage());
        }
    }

    protected void addcompleters(final Completer... completers) {
        for (final Completer completer : completers) {
            consoleReader.addCompleter(completer);
        }
    }

    protected void removeAllcompleters() {
        final LinkedList completers = new LinkedList(consoleReader.getCompleters());
        for (final Object completer : completers) {
            consoleReader.removeCompleter((Completer) completer);
        }
    }

    protected void removecompleters(final Completer... completers) {
        for (final Completer completer : completers) {
            consoleReader.removeCompleter(completer);
        }
    }

    public void loop() throws ExitSignal  {
        loop(DEFAULT_PROMPT);
    }

    public void loop(final String prompt) throws ExitSignal {
        while (!shutdown) {
            try {
                final String line = read(prompt);
                evaluate(line);
            } catch (final Exception ex) {
                if (ex instanceof ExitSignal) {
                    throw (ExitSignal) ex;
                } else {
                    throw new ExitSignal(1, ex.getMessage());
                }
            }
        }
    }

    protected String read(final String prompt) throws IOException {
        return consoleReader.readLine(prompt).trim();
    }

    protected CharSequence popHistory() {
        if (consoleReader.getHistory().isEmpty()) {
            return null;
        } else {
            return consoleReader.getHistory().removeLast();
        }
    }

    protected void pushHistory(final String command) {
        consoleReader.getHistory().add(command);
    }

    protected Iterable<String> history() {
        return Lists.newArrayList(Iterators.transform(consoleReader.getHistory().entries(), new Function<History.Entry, String>() {
            @Override
            public String apply(final History.Entry entry) {
                return entry.value().toString();
            }
        }));
    }

    protected void outputColumns(final int width, final String... cols) {
        outputColumns(width, Lists.newArrayList(cols));
    }

    protected void outputColumns(final int width, final Iterable<String> cols) {
        try {
            consoleReader.println(SPACE_JOINER.join(formatCols(width, cols)));
        } catch (final IOException ex) {
            throw new ExitSignal(1, ex.getMessage());
        }
    }

    private Iterable<String> formatCols(final int width, final Iterable<String> cols) {
        return Iterables.transform(cols, new Function<String, String>() {
            @Override
            public String apply(final String input) {
                return format("%-" + width + "s", input);
            }
        });
    }

    protected void output(final String message, final Object... args) {
        try {
            consoleReader.println(format(message, args));
        } catch (final IOException ex) {
            throw new ExitSignal(1, ex.getMessage());
        }
    }

    protected void error(final String message, final Object... args) {
        try {
            consoleReader.println(format(message, args));
        } catch (final IOException ex) {
            throw new ExitSignal(1, ex.getMessage());
        }
    }

    protected void shutdown() {
        this.shutdown = true;
    }

    abstract protected void evaluate(final String cmd) throws ExitSignal;

    public static class ExitSignal extends RuntimeException {

        private final String message;
        private final int exitCode;

        public ExitSignal(final int exitCode, final String message) {
            this.exitCode = exitCode;
            this.message = message;
        }

        public int getExitCode() {
            return exitCode;
        }

        public String getMessage() {
            return message;
        }
    }
}
