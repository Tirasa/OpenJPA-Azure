/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.    
 */
package org.apache.openjpa.jdbc.kernel;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.openjpa.azure.jdbc.AzureDelegatingConnection;
import org.apache.openjpa.azure.jdbc.AzurePreparedStatement;
import org.apache.openjpa.jdbc.meta.ClassMapping;
import org.apache.openjpa.jdbc.meta.MappingRepository;
import org.apache.openjpa.jdbc.meta.QueryResultMapping;
import org.apache.openjpa.jdbc.sql.DBDictionary;
import org.apache.openjpa.jdbc.sql.ResultSetResult;
import org.apache.openjpa.jdbc.sql.SQLBuffer;
import org.apache.openjpa.jdbc.sql.SQLExceptions;
import org.apache.openjpa.kernel.AbstractStoreQuery;
import org.apache.openjpa.kernel.QueryContext;
import org.apache.openjpa.kernel.StoreQuery;
import org.apache.openjpa.lib.rop.RangeResultObjectProvider;
import org.apache.openjpa.lib.rop.ResultObjectProvider;
import org.apache.openjpa.lib.util.Localizer;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.util.UserException;

/**
 * A SQL query for Azure.
 */
public class AzureSqlStoreQuery extends SQLStoreQuery {

    private static final Localizer _loc = Localizer.forPackage(SQLStoreQuery.class);

    private static final long serialVersionUID = -1937595849088660476L;

    private transient final JDBCStore _store;

    /**
     * Construct a query managed by the given context.
     */
    public AzureSqlStoreQuery(final JDBCStore store) {
        super(store);
        _store = store;
    }

    @Override
    public JDBCStore getStore() {
        return _store;
    }

    @Override
    public boolean supportsParameterDeclarations() {
        return false;
    }

    @Override
    public boolean supportsDataStoreExecution() {
        return true;
    }

    @Override
    public StoreQuery.Executor newDataStoreExecutor(final ClassMetaData meta, final boolean subclasses) {
        return new AzureSqlStoreQuery.SQLExecutor(this, meta);
    }

    @Override
    public boolean requiresCandidateType() {
        return false;
    }

    @Override
    public boolean requiresParameterDeclarations() {
        return false;
    }

    /**
     * Executes the filter as a SQL query.
     */
    public static class SQLExecutor
            extends AbstractStoreQuery.AbstractExecutor {

        private final ClassMetaData _meta;

        private final boolean _select;

        private final boolean _call;   // native call stored procedure

        private final QueryResultMapping _resultMapping;

        public SQLExecutor(final AzureSqlStoreQuery query, final ClassMetaData candidate) {
            final QueryContext ctx = query.getContext();
            final String resultMapping = ctx.getResultMappingName();
            if (resultMapping == null) {
                _resultMapping = null;
            } else {
                final ClassLoader envLoader = ctx.getStoreContext().getClassLoader();
                final MappingRepository repos = query.getStore().getConfiguration().getMappingRepositoryInstance();
                _resultMapping = repos.
                        getQueryResultMapping(ctx.getResultMappingScope(), resultMapping, envLoader, true);
            }
            _meta = candidate;

            final String sql = StringUtils.trimToNull(ctx.getQueryString());
            if (sql == null) {
                throw new UserException(_loc.get("no-sql"));
            }
            _select = query.getStore().getDBDictionary().isSelect(sql);
            _call = sql.length() > 4 && sql.substring(0, 4).equalsIgnoreCase("call");
        }

        @Override
        public int getOperation(final StoreQuery query) {
            return _select ? OP_SELECT
                    : (query.getContext().getCandidateType() != null
                    || query.getContext().getResultType() != null
                    || query.getContext().getResultMappingName() != null
                    || query.getContext().getResultMappingScope() != null)
                    ? OP_SELECT : OP_UPDATE;
        }

        @Override
        public Number executeUpdate(final StoreQuery query, final Object[] params) {
            final JDBCStore store = ((SQLStoreQuery) query).getStore();
            final DBDictionary dict = store.getDBDictionary();
            final String sql = query.getContext().getQueryString();

            final List paramList = new ArrayList(Arrays.asList(params));
            final SQLBuffer buf = new SQLBuffer(dict).append(sql);

            // we need to make sure we have an active store connection
            store.getContext().beginStore();

            final Connection conn = store.getConnection();

            ((AzureDelegatingConnection) ((AzureStoreManager.AzureRefCountConnection) conn).getConn()).
                    selectWorkingConnections();

            final JDBCFetchConfiguration fetch = (JDBCFetchConfiguration) query.getContext().getFetchConfiguration();

            PreparedStatement stmt = null;
            try {
                if (_call) {
                    stmt = prepareCall(conn, buf);
                } else {
                    stmt = prepareStatement(conn, buf);
                }

                buf.setParameters(paramList);
                if (stmt != null) {
                    buf.setParameters(stmt);
                }

                dict.setTimeouts(stmt, fetch, true);

                int count = executeUpdate(store, conn, stmt, buf);

                return count;
            } catch (SQLException se) {
                throw SQLExceptions.getStore(se, dict);
            } finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException se) {
                        // safe to ignore
                    } finally {
                        stmt = null;
                    }
                }
                try {
                    conn.close();
                } catch (SQLException se) {
                }
            }
        }

        @Override
        public ResultObjectProvider executeQuery(final StoreQuery query, final Object[] params,
                final StoreQuery.Range range) {

            final JDBCStore store = ((SQLStoreQuery) query).getStore();
            final DBDictionary dict = store.getDBDictionary();
            final String sql = query.getContext().getQueryString();

            final List paramList = new ArrayList(Arrays.asList(params));
            final SQLBuffer buf = new SQLBuffer(dict).append(sql);

            final Connection conn = store.getConnection();

            ((AzureDelegatingConnection) ((AzureStoreManager.AzureRefCountConnection) conn).getConn()).
                    selectWorkingConnections();

            final JDBCFetchConfiguration fetch = (JDBCFetchConfiguration) query.getContext().getFetchConfiguration();

            ResultObjectProvider rop;
            PreparedStatement stmt = null;
            try {
                // use the right method depending on sel vs. proc, lrs setting
                if (_select && !range.lrs) {
                    stmt = prepareStatement(conn, buf);
                } else if (_select) {
                    stmt = prepareStatement(conn, buf, fetch, -1, -1);
                } else if (!range.lrs) {
                    stmt = prepareCall(conn, buf);
                } else {
                    stmt = prepareCall(conn, buf, fetch, -1, -1);
                }

                int index = 0;
                for (Iterator i = paramList.iterator(); i.hasNext() && stmt != null;) {
                    dict.setUnknown(stmt, ++index, i.next(), null);
                }

                dict.setTimeouts(stmt, fetch, false);
                final ResultSet rs = executeQuery(store, conn, stmt, buf, paramList);

                final ResultSetResult res = stmt != null
                        ? new ResultSetResult(conn, stmt, rs, store) : new ResultSetResult(conn, rs, dict);

                if (_resultMapping != null) {
                    rop = new MappedQueryResultObjectProvider(_resultMapping, store, fetch, res);
                } else if (query.getContext().getCandidateType() != null) {
                    rop = new GenericResultObjectProvider((ClassMapping) _meta, store, fetch, res);
                } else {
                    rop = new SQLProjectionResultObjectProvider(store, fetch, res, query.getContext().getResultType());
                }
            } catch (SQLException se) {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException se2) {
                    }
                }
                try {
                    conn.close();
                } catch (SQLException se2) {
                }
                throw SQLExceptions.getStore(se, dict);
            }

            if (range.start != 0 || range.end != Long.MAX_VALUE) {
                rop = new RangeResultObjectProvider(rop, range.start, range.end);
            }
            return rop;
        }

        @Override
        public String[] getDataStoreActions(final StoreQuery query, final Object[] params,
                final StoreQuery.Range range) {

            return new String[]{query.getContext().getQueryString()};
        }

        @Override
        public boolean isPacking(final StoreQuery query) {
            return query.getContext().getCandidateType() == null;
        }

        /**
         * This method is to provide override for non-JDBC or JDBC-like implementation of preparing call statement.
         */
        protected PreparedStatement prepareCall(final Connection conn, final SQLBuffer buf)
                throws SQLException {

            return buf.prepareCall(conn);
        }

        /**
         * This method is to provide override for non-JDBC or JDBC-like implementation of executing update.
         */
        protected int executeUpdate(final JDBCStore store, final Connection conn,
                final PreparedStatement stmt, final SQLBuffer buf)
                throws SQLException {

            int count = 0;

            if (_call && stmt.execute() == false) {
                count = stmt.getUpdateCount();
            } else {
                if (stmt instanceof AzurePreparedStatement) {
                    // native insert, update, delete
                    count = ((AzurePreparedStatement) stmt).executeUpdate();
                } else {
                    count = stmt.executeUpdate();
                }
            }
            return count;
        }

        /**
         * This method is to provide override for non-JDBC or JDBC-like implementation of preparing call statement.
         */
        protected PreparedStatement prepareCall(final Connection conn, final SQLBuffer buf,
                final JDBCFetchConfiguration fetch, final int rsType, final int rsConcur)
                throws SQLException {

            return buf.prepareCall(conn, fetch, rsType, rsConcur);
        }

        /**
         * This method is to provide override for non-JDBC or JDBC-like implementation of preparing statement.
         */
        protected PreparedStatement prepareStatement(final Connection conn, final SQLBuffer buf)
                throws SQLException {

            return buf.prepareStatement(conn);
        }

        /**
         * This method is to provide override for non-JDBC or JDBC-like implementation of preparing statement.
         */
        protected PreparedStatement prepareStatement(final Connection conn, final SQLBuffer buf,
                final JDBCFetchConfiguration fetch, final int rsType, final int rsConcur)
                throws SQLException {

            return buf.prepareStatement(conn, fetch, rsType, rsConcur);
        }

        /**
         * This method is to provide override for non-JDBC or JDBC-like implementation of executing query.
         */
        protected ResultSet executeQuery(final JDBCStore store, final Connection conn,
                final PreparedStatement stmt, final SQLBuffer buf, final List paramList)
                throws SQLException {

            return (stmt instanceof AzurePreparedStatement)
                    ? ((AzurePreparedStatement) stmt).executeQuery()
                    : stmt.executeQuery();

        }

        /**
         * The given query is parsed to find the parameter tokens of the form
         * <code>?n</code> which is different than
         * <code>?</code> tokens in actual SQL parameter tokens. These
         * <code>?n</code> style tokens are replaced in the query string by
         * <code>?</code> tokens.
         *
         * During the token parsing, the ordering of the tokens is recorded. The given userParam must contain parameter
         * keys as Integer and the same Integers must appear in the tokens.
         *
         */
        @Override
        public Object[] toParameterArray(final StoreQuery query, Map userParams) {
            if (userParams == null || userParams.isEmpty()) {
                return StoreQuery.EMPTY_OBJECTS;
            }
            String sql = query.getContext().getQueryString();
            List<Integer> paramOrder = new ArrayList<Integer>();
            try {
                sql = substituteParams(sql, paramOrder);
            } catch (IOException ex) {
                throw new UserException(ex.getLocalizedMessage());
            }

            Object[] result = new Object[paramOrder.size()];
            int idx = 0;
            for (Integer key : paramOrder) {
                if (!userParams.containsKey(key)) {
                    throw new UserException(_loc.get("uparam-missing",
                            key, sql, userParams));
                }
                result[idx++] = userParams.get(key);
            }
            // modify original JPA-style SQL to proper SQL
            query.getContext().getQuery().setQuery(sql);
            return result;
        }
    }

    /**
     * Utility method to substitute '?num' for parameters in the given SQL statement, and fill-in the order of the
     * parameter tokens
     */
    public static String substituteParams(String sql, List<Integer> paramOrder)
            throws IOException {
        // if there's no "?" parameter marker, then we don't need to
        // perform the parsing process
        if (sql.indexOf("?") == -1) {
            return sql;
        }

        paramOrder.clear();
        StreamTokenizer tok = new StreamTokenizer(new StringReader(sql));
        tok.resetSyntax();
        tok.quoteChar('\'');
        tok.wordChars('0', '9');
        tok.wordChars('?', '?');

        StringBuilder buf = new StringBuilder(sql.length());
        for (int ttype; (ttype = tok.nextToken())
                != StreamTokenizer.TT_EOF;) {
            switch (ttype) {
                case StreamTokenizer.TT_WORD:
                    // a token is a positional parameter if it starts with
                    // a "?" and the rest of the token are all numbers
                    if (tok.sval.startsWith("?")) {
                        buf.append("?");
                        String pIndex = tok.sval.substring(1);
                        if (pIndex.length() > 0) {
                            paramOrder.add(Integer.valueOf(pIndex));
                        } else { // or nothing
                            paramOrder.add(paramOrder.size() + 1);
                        }
                    } else {
                        buf.append(tok.sval);
                    }
                    break;
                case '\'':
                    buf.append('\'');
                    if (tok.sval != null) {
                        buf.append(tok.sval);
                        buf.append('\'');
                    }
                    break;
                default:
                    buf.append((char) ttype);
            }
        }
        return buf.toString();
    }
}
