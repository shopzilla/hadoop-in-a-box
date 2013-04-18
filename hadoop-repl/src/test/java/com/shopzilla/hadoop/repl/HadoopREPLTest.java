/**
 * Copyright (C) 2004 - 2013 Shopzilla, Inc. 
 * All rights reserved. Unauthorized disclosure or distribution is prohibited.
 */

package com.shopzilla.hadoop.repl;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.shopzilla.hadoop.repl.commands.Call;
import com.shopzilla.hadoop.repl.commands.Command;
import com.shopzilla.hadoop.repl.commands.CommandInvocation;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static com.shopzilla.hadoop.repl.commands.Call.call;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * @author Jeremy Lucas
 * @since 4/17/13
 */
public class HadoopREPLTest {
    @Test
    public void testEvaluate() throws Exception {
        final Configuration configuration = mock(Configuration.class);
        final SessionState sessionState = mock(SessionState.class);

        final HadoopREPL repl = new HadoopREPL(configuration, sessionState, ImmutableMap.<Call, Command>of(
            call("test"), new Command() {
            @Override
            public void execute(final CommandInvocation call, final SessionState ss) throws REPL.ExitSignal {
                assertEquals("test", call.command());
                assertArrayEquals(new String[0], call.args());
                assertEquals(sessionState, ss);
            }

            @Override
            public Usage usage(final SessionState sessionState) {
                return null;
            }
        }
        ));

        repl.evaluate("test");

        assertEquals("test", Iterables.get(repl.history(), 0));
    }

    @Test
    public void testEvaluateHelpWithUsage() throws Exception {
        final Configuration configuration = new Configuration();
        final SessionState sessionState = mock(SessionState.class);
        when(sessionState.configuration()).thenReturn(configuration);
        final HadoopREPL repl = new HadoopREPL(configuration, sessionState);

        doAnswer(new Answer() {
            @Override
            public Object answer(final InvocationOnMock invocationOnMock) throws Throwable {
                assertEquals("Displaying help for \"save\"", String.format(invocationOnMock.getArguments()[0].toString(), invocationOnMock.getArguments()[1]));
                return null;
            }
        }).when(sessionState).output(anyString());

        doAnswer(new Answer() {
            @Override
            public Object answer(final InvocationOnMock invocationOnMock) throws Throwable {
                final Command.Usage usage = (Command.Usage) invocationOnMock.getArguments()[0];
                assertEquals("save", usage.command);
                return null;
            }
        }).when(sessionState).outputUsage(any(Command.Usage.class));

        repl.evaluate("help save");

        assertEquals("help save", Iterables.get(repl.history(), 0));
    }

    @Test
    public void testEvaluateHelpWithNoSuchCommandUsage() throws Exception {
        final Configuration configuration = new Configuration();
        final SessionState sessionState = mock(SessionState.class);
        when(sessionState.configuration()).thenReturn(configuration);
        final HadoopREPL repl = new HadoopREPL(configuration, sessionState);

        doAnswer(new Answer() {
            @Override
            public Object answer(final InvocationOnMock invocationOnMock) throws Throwable {
                assertEquals("Unknown command \"test\"", String.format(invocationOnMock.getArguments()[0].toString(), invocationOnMock.getArguments()[1]));
                return null;
            }
        }).when(sessionState).error(anyString());

        repl.evaluate("help test");

        assertEquals("help test", Iterables.get(repl.history(), 0));
    }

    @Test
    public void testEvaluateUnknownCommand() throws Exception {
        final Configuration configuration = mock(Configuration.class);
        final SessionState sessionState = mock(SessionState.class);

        doAnswer(new Answer() {
            @Override
            public Object answer(final InvocationOnMock invocationOnMock) throws Throwable {
                assertEquals("Unknown command \"test\"", String.format(invocationOnMock.getArguments()[0].toString(), invocationOnMock.getArguments()[1]));
                return null;
            }
        }).when(sessionState).output(anyString(), any(Object[].class));

        final HadoopREPL repl = new HadoopREPL(configuration, sessionState, ImmutableMap.<Call, Command>of());

        repl.evaluate("test");

        assertEquals("test", Iterables.get(repl.history(), 0));
    }
}
