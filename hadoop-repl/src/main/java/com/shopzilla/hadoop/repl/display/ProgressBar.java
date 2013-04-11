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
package com.shopzilla.hadoop.repl.display;

import java.text.DecimalFormat;

/**
 * @author Jeremy Lucas
 * @since 4/11/13
 */
public class ProgressBar {
    private static final DecimalFormat DF = new DecimalFormat("##%");

    protected final int total;

    public ProgressBar(final int total) {
        this.total = total;
    }

    public String progress(final int i) {
        final StringBuilder sb = new StringBuilder("\r[");
        for (int c = 0; c < i; c++) {
            sb.append('=');
        }
        sb.append(">");
        for (int c = i; c < total; c++) {
            sb.append(' ');
        }
        return sb.append("] ").append(DF.format((double) i / total)).toString();
    }
}
