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
package org.apache.openjpa.jdbc.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.openjpa.conf.OpenJPAConfiguration;
import org.apache.openjpa.federation.jdbc.Federation;
import org.apache.openjpa.federation.jdbc.SQLAzureConfiguration;
import org.apache.openjpa.jdbc.conf.JDBCConfiguration;
import org.apache.openjpa.jdbc.kernel.JDBCFetchConfiguration;
import org.apache.openjpa.jdbc.kernel.JDBCLockManager;
import org.apache.openjpa.jdbc.kernel.JDBCStore;
import org.apache.openjpa.jdbc.meta.FieldMapping;
import org.apache.openjpa.kernel.StoreContext;
import org.apache.openjpa.lib.log.Log;
import org.apache.openjpa.utils.SQLAzureUtils;

public class SQLAzureSelectImpl extends SelectImpl {

    private final JDBCConfiguration _conf;

    private final DBDictionary _dict;

    private final Log _log;

    public SQLAzureSelectImpl(final JDBCConfiguration conf) {
        super(conf);
        this._conf = conf;
        this._dict = conf.getDBDictionaryInstance();
        this._log = conf.getLog(OpenJPAConfiguration.LOG_RUNTIME);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Result execute(
            final StoreContext ctx, final JDBCStore store, final JDBCFetchConfiguration fetch, final int lockLevel)
            throws SQLException {

        ResultSet[] resultSets2 = new ResultSet[2];

        boolean forUpdate = false;
        if (!isAggregate() && getGrouping() == null) {
            JDBCLockManager lm = store.getLockManager();
            if (lm != null) {
                forUpdate = lm.selectForUpdate(this, lockLevel);
            }
        }

        logEagerRelations();
        SQLBuffer sql = toSelect(forUpdate, fetch);
        boolean isLRS = isLRS();
        int rsType = (isLRS && supportsRandomAccess(forUpdate)) ? -1 : ResultSet.TYPE_FORWARD_ONLY;

        List<ResultSet> resultSets = new ArrayList<ResultSet>();
        List<Statement> statements = new ArrayList<Statement>();

        final Connection conn = store.getConnection();

        SQLAzureUtils.useRootFederation(conn);
        execute(conn, sql, store, fetch, rsType, isLRS, forUpdate, resultSets, statements);

        final Collection<Federation> federations = ((SQLAzureConfiguration) _conf).getFederations();
        if (federations != null) {
            for (Federation federation : federations) {
                for (String id : SQLAzureUtils.getMemberDistribution(conn, federation)) {
                    SQLAzureUtils.useFederation(conn, federation.getName(), id);
                    execute(conn, sql, store, fetch, rsType, isLRS, forUpdate, resultSets, statements);
                }
            }
        }

        if (statements.isEmpty() && conn != null) {
            try {
                conn.close();
            } catch (SQLException ignore) {
                // ignore exception
            }
        }

        return getEagerResult(conn, statements, resultSets, store, fetch, forUpdate, sql);
    }

    private void execute(
            final Connection conn,
            final SQLBuffer sql,
            final JDBCStore store,
            final JDBCFetchConfiguration fetch,
            final int rsType,
            final boolean isLRS,
            final boolean forUpdate,
            final List<ResultSet> resultSets,
            final List<Statement> statements) {

        PreparedStatement stmnt = null;

        try {
            stmnt = isLRS
                    ? prepareStatement(conn, sql, fetch, rsType, -1, true)
                    : prepareStatement(conn, sql, null, rsType, -1, false);

            getDictionary().setTimeouts(stmnt, fetch, forUpdate);

            resultSets.add(executeQuery(conn, stmnt, sql, isLRS, store));
            statements.add(stmnt);

        } catch (SQLException e) {
            _log.trace("Error executing query: " + e.getMessage());

            // clean up statement
            if (stmnt != null) {
                try {
                    stmnt.close();
                } catch (SQLException ignore) {
                    // ignore exception
                }
            }
        }
    }

    /**
     * Execute our eager selects, adding the results under the same keys to the given result.
     */
    private static void addEagerResults(final SQLAzureResultSetResult res, final SQLAzureSelectImpl sel,
            JDBCStore store, JDBCFetchConfiguration fetch)
            throws SQLException {
        if (sel.getEagerMap() == null) {
            return;
        }

        // execute eager selects
        Map.Entry entry;
        Result eres;
        Map eager;
        for (Iterator itr = sel.getEagerMap().entrySet().iterator(); itr.hasNext();) {
            entry = (Map.Entry) itr.next();

            // simulated batched selects for inner/outer joins; for separate
            // selects, don't pass on lock level, because they're probably
            // for relations and therefore should use default level
            if (entry.getValue() == sel) {
                eres = res;
            } else {
                eres = ((SelectExecutor) entry.getValue()).execute(store,
                        fetch);
            }

            eager = res.getEagerMap(false);
            if (eager == null) {
                eager = new HashMap();
                res.setEagerMap(eager);
            }
            eager.put(entry.getKey(), eres);
        }
    }

    /**
     * This method is to provide override for non-JDBC or JDBC-like implementation of executing eager selects.
     */
    public Result getEagerResult(
            final Connection connection,
            final List<Statement> statements,
            final List<ResultSet> rs,
            final JDBCStore store,
            final JDBCFetchConfiguration fetch,
            final boolean forUpdate,
            final SQLBuffer sql)
            throws SQLException {

        SQLAzureResultSetResult res = new SQLAzureResultSetResult(connection, statements, rs, _dict);
        res.setSelect(this);
        res.setStore(store);
        res.setLocking(forUpdate);
        try {
            addEagerResults(res, this, store, fetch);
        } catch (SQLException se) {
            res.close();
            throw se;
        }
        return res;
    }

    /**
     * Return the eager key to use for the user-given key.
     */
    private static Object toEagerKey(FieldMapping key, PathJoins pj) {
        if (pj == null || pj.path() == null) {
            return key;
        }
        return new Key(pj.path().toString(), key);
    }

    /**
     * Key type used for aliases.
     */
    private static class Key {

        private final String _path;

        private final Object _key;

        public Key(String path, Object key) {
            _path = path;
            _key = key;
        }

        public int hashCode() {
            return ((_path == null) ? 0 : _path.hashCode()) ^ ((_key == null) ? 0 : _key.hashCode());
        }

        public boolean equals(Object other) {
            if (other == null) {
                return false;
            }
            if (other == this) {
                return true;
            }
            if (other.getClass() != getClass()) {
                return false;
            }

            final SQLAzureSelectImpl.Key k = (SQLAzureSelectImpl.Key) other;

            if (k._key == null || k._path == null || _key == null || _path == null) {
                return false;
            }

            return k._path.equals(_path) && k._key.equals(_key);
        }

        public String toString() {
            return _path + "|" + _key;
        }

        Object getKey() {
            return _key;
        }
    }
}
