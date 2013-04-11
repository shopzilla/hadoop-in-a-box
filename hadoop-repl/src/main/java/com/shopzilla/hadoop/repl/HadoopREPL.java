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

import com.google.common.base.CharMatcher;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.*;
import com.shopzilla.hadoop.repl.commands.*;
import com.shopzilla.hadoop.repl.commands.completers.DeferredStringsCompleter;
import jline.console.completer.AggregateCompleter;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.util.Map;
import java.util.TreeSet;

import static com.shopzilla.hadoop.repl.commands.Call.call;
import static java.lang.String.format;

/**
 * @author Jeremy Lucas
 * @since 9/12/12
 */
public class HadoopREPL extends REPL {

    protected static final Splitter ARG_SPLITTER = Splitter.on(CharMatcher.WHITESPACE).omitEmptyStrings();

    protected final Configuration configuration;

    protected SessionState sessionState;

    protected final Map<Call, Command> commandMappings;

    public HadoopREPL(final Configuration configuration) throws ExitSignal {
        this.configuration = configuration;
        this.sessionState = new SessionState(configuration, this);
        this.commandMappings = buildCommandMappings();
        resetCompletors();
    }

    protected Map<Call, Command> buildCommandMappings() {
        final Map<Call, Command> commands = ImmutableMap.<Call, Command>builder()
            .putAll(new SessionCommandProvider().apply(sessionState))
            .putAll(new FSShellCommandProvider().apply(sessionState))
            .build();
        return ImmutableMap.<Call, Command>builder()
            .putAll(commands)
            .put(call("help", new DeferredStringsCompleter<Map<Call, Command>>(commandMappings, new Function<Map<Call, Command>, TreeSet<String>>() {
                @Override
                public TreeSet<String> apply(final Map<Call, Command> calls) {
                    return Sets.newTreeSet(Maps.transformEntries(commandMappings, new Maps.EntryTransformer<Call, Command, String>() {
                        @Override
                        public String transformEntry(final Call key, final Command value) {
                            return key.commandName;
                        }
                    }).values());
                }
            })), new Command() {
                @Override
                public void execute(final CommandInvocation call, final SessionState sessionState) throws REPL.ExitSignal {
                    if (call.args.length != 1) {
                        sessionState.output("Usage: help [command]");
                    } else {
                        final String command = call.args[0];
                        if (commandMappings.containsKey(call(command))) {
                            final Usage usage = commandMappings.get(call(command)).usage(sessionState);
                            sessionState.output(format("Displaying help for \"%s\":\n", command));
                            sessionState.outputUsage(usage);
                        } else {
                            sessionState.error(format("Unknown command \"%s\"", command));
                        }
                    }
                }

                @Override
                public Usage usage(final SessionState sessionState) {
                    return new Usage(
                        "help",
                        "Displays help / usage information for the given command ",
                        "<command>"
                    );
                }
            })
            .build();
    }

    protected void resetCompletors() {
        removeAllcompleters();
        addcompleters(new AggregateCompleter(
            Collections2.transform(commandMappings.keySet(), new Function<Call, Completer>() {
                @Override
                public Completer apply(final Call input) {
                    return new ArgumentCompleter(
                        ImmutableList.<Completer>builder()
                            .add(new StringsCompleter(input.commandName))
                            .add(input.completers)
                            .build()
                    );
                }
            })
        ));
    }

    @Override
    protected void evaluate(final String input) throws ExitSignal {
        final Iterable<String> inputParts = ARG_SPLITTER.limit(2).split(input);
        final String command = Iterables.get(inputParts, 0).toLowerCase();
        if (command.isEmpty()) {
             // Do nothing
        } else if (commandMappings.containsKey(call(command))) {
            commandMappings.get(call(command)).execute(
                new CommandInvocation(command, Iterables.toArray(ARG_SPLITTER.split(Iterables.get(inputParts, 1, "")), String.class)),
                sessionState
            );
        } else {
            output("Unknown command \"%s\"", command);
        }
    }

    public static void main(final String[] args) {
        int exitCode = 0;

        try {
            if (args.length < 1) {
                throw new ExitSignal(1, "Usage: ./hadoop-repl <path-to-hadoop-core-site-file>");
            }
            final Configuration configuration = new Configuration(true);
            configuration.addResource(new File(args[0]).toURI().toURL());
            new HadoopREPL(configuration).loop("hadoop> ");
        } catch (final ExitSignal ex) {
            System.err.println(ex.getMessage());
            exitCode = ex.getExitCode();
        } catch (final Exception ex) {
            System.err.println(ex);
            exitCode = 1;
        }

        System.exit(exitCode);
    }
}
