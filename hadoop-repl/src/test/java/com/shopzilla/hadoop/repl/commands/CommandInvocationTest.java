/**
 * Copyright (C) 2004 - 2013 Shopzilla, Inc. 
 * All rights reserved. Unauthorized disclosure or distribution is prohibited.
 */

package com.shopzilla.hadoop.repl.commands;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * @author Jeremy Lucas
 * @since 4/17/13
 */
public class CommandInvocationTest {
    @Test
    public void testArgsImmutability() throws Exception {
        final CommandInvocation commandInvocation = new CommandInvocation("cmd", "a", "t");
        commandInvocation.args()[1] = "b";
        assertArrayEquals(new String[] { "a", "t" }, commandInvocation.args());
    }
}
