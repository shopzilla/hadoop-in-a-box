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

package com.shopzilla.hadoop.repl.commands.completers;

import com.google.common.base.Function;
import jline.console.completer.Completer;

import java.util.List;
import java.util.TreeSet;

import static jline.internal.Preconditions.checkNotNull;

/**
 * @author Jeremy Lucas
 * @since 4/11/13
 */
public class DeferredStringsCompleter<T> implements Completer {

    protected final T target;

    protected final Function<T, TreeSet<String>> stringsProvider;

    public DeferredStringsCompleter(final T target, final Function<T, TreeSet<String>> stringsProvider) {
        this.target = target;
        this.stringsProvider = stringsProvider;
    }

    public TreeSet<String> getStrings() {
        return stringsProvider.apply(target);
    }

    public int complete(final String buffer, final int cursor, final List<CharSequence> candidates) {
        // buffer could be null
        checkNotNull(candidates);

        if (buffer == null) {
            candidates.addAll(getStrings());
        }
        else {
            for (String match : getStrings().tailSet(buffer)) {
                if (!match.startsWith(buffer)) {
                    break;
                }

                candidates.add(match);
            }
        }

        if (candidates.size() == 1) {
            candidates.set(0, candidates.get(0) + " ");
        }

        return candidates.isEmpty() ? -1 : 0;
    }
}
