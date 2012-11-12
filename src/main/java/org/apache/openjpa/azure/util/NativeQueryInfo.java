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
import java.util.List;
import java.util.StringTokenizer;
import org.apache.commons.lang.StringUtils;

public class NativeQueryInfo {

    public enum StatementType {

        INSERT,
        DELETE,
        UPDATE,
        SELECT
    };

    private StatementType type;

    private List<String> tableNames = new ArrayList<String>();

    public NativeQueryInfo(final String query) {

        try {

            final StringTokenizer tokenizer = new StringTokenizer(query);


            type = StatementType.valueOf(tokenizer.nextToken().toUpperCase());

            switch (type) {
                case INSERT:
                    String tableName = tokenizer.nextToken();

                    // INTO is optional
                    if ("INTO".equalsIgnoreCase(tableName)) {
                        tableName = tokenizer.nextToken();
                    }

                    int lastPoint;

                    if ((lastPoint = tableName.lastIndexOf(".")) > 0) {
                        tableNames.add(tableName.substring(lastPoint, tableName.length()));
                    } else {
                        tableNames.add(tableName);
                    }

                    break;
                case DELETE:
                    tableName = tokenizer.nextToken();

                    // FROM is optional
                    if ("FROM".equalsIgnoreCase(tableName)) {
                        tableName = tokenizer.nextToken();
                    }

                    if ((lastPoint = tableName.lastIndexOf(".")) > 0) {
                        tableNames.add(tableName.substring(lastPoint, tableName.length()));
                    } else {
                        tableNames.add(tableName);
                    }

                    break;
                case UPDATE:
                    tableName = tokenizer.nextToken();

                    if ((lastPoint = tableName.lastIndexOf(".")) > 0) {
                        tableNames.add(tableName.substring(lastPoint, tableName.length()));
                    } else {
                        tableNames.add(tableName);
                    }

                    break;
                case SELECT:
                    // table list start from "FROM" word
                    if (StringUtils.isNotBlank(query)) {
                        int from = query.indexOf("FROM");
                        int to = query.indexOf("WHERE");

                        if (from > 0) {
                            tableNames = getTableNames(query.substring(from + 5, to < 0 ? query.length() : to));
                        }
                    }
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid query " + query);
        }
    }

    public List<String> getTableNames() {
        return tableNames;
    }

    public StatementType getType() {
        return type;
    }

    private List<String> getTableNames(final String fromClause) {
        final List<String> result = new ArrayList<String>();

        if (StringUtils.isNotBlank(fromClause)) {
            for (String from : fromClause.split(",")) {
                String name = from.trim();
                result.add(name.substring(0, name.contains(" ") ? name.indexOf(" ") : name.length()));
            }
        }

        return result;
    }
}
