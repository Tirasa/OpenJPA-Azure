/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.openjpa.azure.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.openjpa.azure.jdbc.conf.AzureConfiguration.RangeType;

public class MemberDistribution implements Iterable<Object> {

    private final RangeType type;

    private final List values = new ArrayList();

    public MemberDistribution(final RangeType type) {
        this.type = type;
    }

    public void addValue(final Object value) {
        values.add(value);
    }

    public int size() {
        return values.size();
    }

    @Override
    public Iterator<Object> iterator() {
        return new MemberIterator();
    }

    private class MemberIterator implements Iterator<Object> {

        Iterator<Object> iter = values.iterator();

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public Object next() {
            final Object value = iter.next();

            switch (type) {
                case BIGINT:
                case INT:
                    return value == null ? 0 : value;
                case UNIQUEIDENTIFIER:
                    return value == null ? "00000000-0000-0000-0000-000000000000" : value;
                case VARBINARY:
                    return value == null ? 0x0 : value;
                default:
                    return null;
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }
}
