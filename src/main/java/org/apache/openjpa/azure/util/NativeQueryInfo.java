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
        SELECT,
        CREATE,
        DROP
    };

    private StatementType type;

    private List<String> tableNames = new ArrayList<String>();

    public NativeQueryInfo(final String query) {

        try {

            final StringTokenizer tokenizer = new StringTokenizer(query);

            type = StatementType.valueOf(tokenizer.nextToken().toUpperCase());

            List<String> objects = new ArrayList<String>();

            switch (type) {
                case DROP:
                    String objectType = tokenizer.nextToken();
                    if ("VIEW".equalsIgnoreCase(objectType)) {
                        // DROP VIEW <view name>
                        objects.add(tokenizer.nextToken());
                    } else if ("INDEX".equalsIgnoreCase(objectType)) {
                        // DROP INDEX <index name> ON <tableName>

                        // index name
                        tokenizer.nextToken();

                        // ON
                        tokenizer.nextToken();

                        // table name
                        objects.add(tokenizer.nextToken());
                    } else {
                        throw new UnsupportedOperationException("Unsupported query " + query);
                    }
                    break;
                case CREATE:
                    objectType = tokenizer.nextToken();

                    if ("VIEW".equalsIgnoreCase(objectType)) {
                        /**
                         * CREATE VIEW [ schema_name . ] view_name [ (column [ ,...n ] ) ] [ WITH <view_attribute> [
                         * ,...n ] ] AS select_statement [ WITH CHECK OPTION ]
                         */
                        objects.add(tokenizer.nextToken());
                    } else {
                        // suppose to have a create index query
                        final String tmp = normalizeCreateIndex(query);

                        /*
                         * NORMALIZED:
                         *
                         * index_name ON <object> (column [ ASC | DESC ] [ ,...n ] ) [ INCLUDE (column_name [ ,...n ] )
                         * ] [ WHERE <filter_predicate> ] [ WITH ( <relational_index_option> [ ,...n ] ) ]
                         */

                        final StringTokenizer indexTokenizer = new StringTokenizer(tmp);

                        // index name
                        indexTokenizer.nextToken();

                        // ON
                        indexTokenizer.nextToken();

                        objects.add(indexTokenizer.nextToken());
                    }

                    break;
                case INSERT:
                    /**
                     * [ WITH <common_table_expression> [ ,...n ] ] INSERT [ TOP ( expression ) [ PERCENT ] ] [ INTO ] {
                     * <object> [ WITH ( <Table_Hint_Limited> [ ...n ] ) ] } { [ ( column_list ) ] [ <OUTPUT Clause> ] {
                     * VALUES ( { DEFAULT | NULL | expression } [ ,...n ] ) [ ,...n ] | derived_table |
                     * execute_statement | <dml_table_source> | DEFAULT VALUES } } [; ]
                     */
                    String tableName = tokenizer.nextToken();

                    // INTO is optional
                    if ("INTO".equalsIgnoreCase(tableName)) {
                        tableName = tokenizer.nextToken();
                    }

                    objects.add(tableName);
                    break;
                case DELETE:
                    tableName = tokenizer.nextToken();

                    // FROM is optional
                    if ("FROM".equalsIgnoreCase(tableName)) {
                        tableName = tokenizer.nextToken();
                    }

                    objects.add(tableName);
                    break;
                case UPDATE:
                    objects.add(tokenizer.nextToken());
                    break;
                case SELECT:
                    // table list start from "FROM" word
                    if (StringUtils.isNotBlank(query)) {
                        int from = query.indexOf("FROM");
                        int to = query.indexOf("WHERE");

                        if (from > 0) {
                            objects.addAll(getTableNames(query.substring(from + 5, to < 0 ? query.length() : to)));
                        }
                    }
            }

            for (String name : objects) {
                final String tableName;
                final int firstParenthesis;

                if ((firstParenthesis = name.indexOf("(")) > 0) {
                    tableName = name.substring(0, firstParenthesis);
                } else {
                    tableName = name;
                }

                final int lastPoint;

                if ((lastPoint = name.lastIndexOf(".")) > 0) {
                    tableNames.add(tableName.substring(lastPoint, tableName.length()));
                } else {
                    tableNames.add(tableName);
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

    private String normalizeCreateIndex(final String query) {
        String input = query.trim();

        if (input.startsWith("CREATE")) {
            /*
             * CREATE [ UNIQUE ] [ CLUSTERED | NONCLUSTERED ] INDEX index_name ON <object> (column [ ASC | DESC ] [
             * ,...n ] ) [ INCLUDE (column_name [ ,...n ] ) ] [ WHERE <filter_predicate> ] [ WITH (
             * <relational_index_option> [ ,...n ] ) ]
             */
            return normalizeCreateIndex(input.substring(6, input.length()));
        } else if (input.startsWith("UNIQUE")) {
            /*
             * ... [ UNIQUE ] [ CLUSTERED | NONCLUSTERED ] INDEX index_name ON <object> (column [ ASC | DESC ] [ ,...n ]
             * ) [ INCLUDE (column_name [ ,...n ] ) ] [ WHERE <filter_predicate> ] [ WITH ( <relational_index_option> [
             * ,...n ] ) ]
             */
            return normalizeCreateIndex(input.substring(6, input.length()));
        } else if (input.startsWith("CLUSTERED")) {
            /*
             * ... [ CLUSTERED] INDEX index_name ON <object> (column [ ASC | DESC ] [ ,...n ] ) [ INCLUDE (column_name [
             * ,...n ] ) ] [ WHERE <filter_predicate> ] [ WITH ( <relational_index_option> [ ,...n ] ) ]
             */
            return normalizeCreateIndex(input.substring(9, input.length()));
        } else if (input.startsWith("NONCLUSTERED")) {
            /*
             * ... [ NONCLUSTERED ] INDEX index_name ON <object> (column [ ASC | DESC ] [ ,...n ] ) [ INCLUDE
             * (column_name [ ,...n ] ) ] [ WHERE <filter_predicate> ] [ WITH ( <relational_index_option> [ ,...n ] ) ]
             */
            return normalizeCreateIndex(input.substring(12, input.length()));
        } else if (input.startsWith("INDEX")) {
            /*
             * ... INDEX index_name ON <object> (column [ ASC | DESC ] [ ,...n ] ) [ INCLUDE (column_name [ ,...n ] ) ]
             * [ WHERE <filter_predicate> ] [ WITH ( <relational_index_option> [ ,...n ] ) ]
             */
            return input.substring(5, input.length()).trim();
        } else {
            throw new UnsupportedOperationException("Unsupported native query " + query);
        }
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
