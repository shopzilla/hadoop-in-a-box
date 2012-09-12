/**
 * Copyright (C) 2004 - 2012 Shopzilla, Inc. 
 * All rights reserved. Unauthorized disclosure or distribution is prohibited.
 */

package com.shopzilla.common.io.console;

import jline.Completor;
import jline.ConsoleReader;

import java.io.IOException;

/**
 * @author Jeremy Lucas
 * @since 9/11/12
 */
public abstract class REPL {

    protected static final String  DEFAULT_PROMPT = "> ";

    private ConsoleReader consoleReader;

    public REPL() throws IOException {
        this.consoleReader = new ConsoleReader();
    }

    protected void addCompletors(final Completor... completors) {
        for (final Completor completor : completors) {
            this.consoleReader.addCompletor(completor);
        }
    }

    protected void loop() throws IOException, ExitSignal  {
        loop(DEFAULT_PROMPT);
    }

    protected void loop(final String prompt) throws IOException, ExitSignal {
        while (true) {
            print(evaluate(read(prompt)));
        }
    }

    protected String read(final String prompt) throws IOException {
        return consoleReader.readLine(prompt).trim();
    }

    abstract protected String evaluate(final String cmd) throws ExitSignal;

    protected void print(final String out) throws ExitSignal {
        System.out.println(out);
    }

    protected static class ExitSignal extends Throwable {

        private final int exitCode;

        public ExitSignal(final int exitCode) {
            this.exitCode = exitCode;
        }

        public int getExitCode() {
            return exitCode;
        }
    }
}
