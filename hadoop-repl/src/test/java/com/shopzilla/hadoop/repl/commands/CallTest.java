/**
 * Copyright (C) 2004 - 2013 Shopzilla, Inc. 
 * All rights reserved. Unauthorized disclosure or distribution is prohibited.
 */

package com.shopzilla.hadoop.repl.commands;

import org.junit.Test;

import static com.shopzilla.hadoop.repl.commands.Call.call;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Jeremy Lucas
 * @since 4/17/13
 */
public class CallTest {
    @Test
    public void testEquals() throws Exception {
        final Call call = call("cmd");
        assertTrue(call.equals(call("cmd")));
    }

    @Test
    public void testNotEquals() throws Exception {
        final Call call = call("cmd");
        assertFalse(call.equals(call("cmd2")));
        assertFalse(call.equals(call("cmd2")));
    }

    @Test
    public void testHashCode() throws Exception {
        final Call call = call("cmd");
        assertTrue(call.equals(call("cmd")));
        assertTrue(call.hashCode() == call("cmd").hashCode());
    }
}
