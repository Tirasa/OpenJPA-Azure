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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.openjpa.jdbc.identifier.DBIdentifier;
import org.apache.openjpa.jdbc.kernel.JDBCStore;
import org.apache.openjpa.jdbc.meta.JavaSQLTypes;
import org.apache.openjpa.jdbc.schema.Column;
import org.apache.openjpa.meta.JavaTypes;

public class SQLAzureResultSetResult extends SelectImpl.SelectResult {

    private final Connection connection;

    private final List<Statement> statements = new ArrayList<Statement>();

    private final List<ResultSet> results = new ArrayList<ResultSet>();

    private int rsIndex = 1;

    private ResultSet _rs;

    private int _row = -1;

    private int _size = -1;

    public SQLAzureResultSetResult(
            final Connection connection,
            final List<Statement> stmnts,
            final List<ResultSet> rs,
            final DBDictionary dict) {

        super(connection,
                stmnts != null && !stmnts.isEmpty() ? stmnts.get(0) : null,
                rs != null && !rs.isEmpty() ? rs.get(0) : null, dict);

        if (rs != null && !rs.isEmpty()) {
            results.addAll(rs);
            _rs = results.get(0);
        }

        this.connection = connection;

        if (stmnts != null) {
            this.statements.addAll(stmnts);
        }
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public ResultSet getResultSet() {
        return _rs;
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public void close() {
        for (ResultSet rs : results) {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException ignore) {
                // ignore exception
            }
        }

        for (Statement stmnt : statements) {
            try {
                if (stmnt != null) {
                    stmnt.close();
                }
            } catch (SQLException ignore) {
                // ignore exception
            }
        }

        if (results != null && !results.isEmpty()) {
            // at least one ResultSet has been valued otherwise nothing to be closed
            super.close();
        }
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public boolean supportsRandomAccess()
            throws SQLException {
        return _rs.getType() != ResultSet.TYPE_FORWARD_ONLY;
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected boolean absoluteInternal(int row)
            throws SQLException {

        if (row == _row + 1) {
            return nextInternal();
        }

        int tmp = row;

        for (ResultSet rs : results) {
            if (rs.absolute(tmp + 1)) {
                _row = row;
                _rs = rs;
                return true;
            } else {
                rs.last();
                tmp -= rs.getRow();
            }
        }

        _row = -1;
        _rs = results.get(results.size() - 1);

        return false;
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected boolean nextInternal()
            throws SQLException {

        _row++;

        boolean res = _rs != null && _rs.next();

        if (!res && rsIndex < results.size()) {
            _rs = results.get(rsIndex);
            rsIndex++;
            res = _rs.next();
        }

        return res;
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public int size()
            throws SQLException {

        if (_size == -1) {
            int localSize = 0;
            for (ResultSet rs : results) {
                rs.last();
                localSize = rs.getRow();

                if (_row == -1) {
                    rs.beforeFirst();
                } else {
                    rs.absolute(_row - _size + 1);
                }

                _size += localSize;
            }
        }

        return _size;
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected Array getArrayInternal(Object obj, Joins joins)
            throws SQLException {
        return getDBDictionary().getArray(_rs, ((Number) obj).intValue());
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected InputStream getAsciiStreamInternal(Object obj, Joins joins)
            throws SQLException {
        return getDBDictionary().getAsciiStream(_rs, ((Number) obj).intValue());
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected BigDecimal getBigDecimalInternal(Object obj, Joins joins)
            throws SQLException {
        return getDBDictionary().getBigDecimal(_rs, ((Number) obj).intValue());
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected Number getNumberInternal(Object obj, Joins joins)
            throws SQLException {
        return getDBDictionary().getNumber(_rs, ((Number) obj).intValue());
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected BigInteger getBigIntegerInternal(Object obj, Joins joins)
            throws SQLException {
        return getDBDictionary().getBigInteger(_rs, ((Number) obj).intValue());
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected InputStream getBinaryStreamInternal(Object obj, Joins joins)
            throws SQLException {
        return getDBDictionary().getBinaryStream(_rs, ((Number) obj).intValue());
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected Blob getBlobInternal(Object obj, Joins joins)
            throws SQLException {
        return getDBDictionary().getBlob(_rs, ((Number) obj).intValue());
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected boolean getBooleanInternal(Object obj, Joins joins)
            throws SQLException {
        return getDBDictionary().getBoolean(_rs, ((Number) obj).intValue());
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected byte getByteInternal(Object obj, Joins joins)
            throws SQLException {
        return getDBDictionary().getByte(_rs, ((Number) obj).intValue());
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected byte[] getBytesInternal(Object obj, Joins joins)
            throws SQLException {
        return getDBDictionary().getBytes(_rs, ((Number) obj).intValue());
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected Calendar getCalendarInternal(Object obj, Joins joins)
            throws SQLException {
        return getDBDictionary().getCalendar(_rs, ((Number) obj).intValue());
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected char getCharInternal(Object obj, Joins joins)
            throws SQLException {
        return getDBDictionary().getChar(_rs, ((Number) obj).intValue());
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected Reader getCharacterStreamInternal(Object obj, Joins joins)
            throws SQLException {
        return getDBDictionary().getCharacterStream(_rs, ((Number) obj).intValue());
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected Clob getClobInternal(Object obj, Joins joins)
            throws SQLException {
        return getDBDictionary().getClob(_rs, ((Number) obj).intValue());
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected Date getDateInternal(Object obj, Joins joins)
            throws SQLException {
        return getDBDictionary().getDate(_rs, ((Number) obj).intValue());
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected java.sql.Date getDateInternal(Object obj, Calendar cal,
            Joins joins)
            throws SQLException {
        return getDBDictionary().getDate(_rs, ((Number) obj).intValue(), cal);
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected double getDoubleInternal(Object obj, Joins joins)
            throws SQLException {
        return getDBDictionary().getDouble(_rs, ((Number) obj).intValue());
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected float getFloatInternal(Object obj, Joins joins)
            throws SQLException {
        return getDBDictionary().getFloat(_rs, ((Number) obj).intValue());
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected int getIntInternal(Object obj, Joins joins)
            throws SQLException {
        return getDBDictionary().getInt(_rs, ((Number) obj).intValue());
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected Locale getLocaleInternal(Object obj, Joins joins)
            throws SQLException {
        return getDBDictionary().getLocale(_rs, ((Number) obj).intValue());
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected long getLongInternal(Object obj, Joins joins)
            throws SQLException {
        return getDBDictionary().getLong(_rs, ((Number) obj).intValue());
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected Object getStreamInternal(JDBCStore store, Object obj,
            int metaTypeCode, Object arg, Joins joins)
            throws SQLException {
        return getLOBStreamInternal(store, obj, joins);
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected Object getObjectInternal(Object obj, int metaTypeCode,
            Object arg, Joins joins)
            throws SQLException {
        if (metaTypeCode == -1 && obj instanceof Column) {
            metaTypeCode = ((Column) obj).getJavaType();
        }

        boolean isClob = (obj instanceof Column) ? ((Column) obj).getType() == Types.CLOB && !((Column) obj).isXML()
                : false;
        obj = translate(obj, joins);

        Object val = null;
        switch (metaTypeCode) {
            case JavaTypes.BOOLEAN:
            case JavaTypes.BOOLEAN_OBJ:
                val = (getBooleanInternal(obj, joins)) ? Boolean.TRUE
                        : Boolean.FALSE;
                break;
            case JavaTypes.BYTE:
            case JavaTypes.BYTE_OBJ:
                val = new Byte(getByteInternal(obj, joins));
                break;
            case JavaTypes.CHAR:
            case JavaTypes.CHAR_OBJ:
                val = new Character(getCharInternal(obj, joins));
                break;
            case JavaTypes.DOUBLE:
            case JavaTypes.DOUBLE_OBJ:
                val = new Double(getDoubleInternal(obj, joins));
                break;
            case JavaTypes.FLOAT:
            case JavaTypes.FLOAT_OBJ:
                val = new Float(getFloatInternal(obj, joins));
                break;
            case JavaTypes.INT:
            case JavaTypes.INT_OBJ:
                val = getIntInternal(obj, joins);
                break;
            case JavaTypes.LONG:
            case JavaTypes.LONG_OBJ:
                val = getLongInternal(obj, joins);
                break;
            case JavaTypes.SHORT:
            case JavaTypes.SHORT_OBJ:
                val = new Short(getShortInternal(obj, joins));
                break;
            case JavaTypes.STRING:
                return getStringInternal(obj, joins, isClob);
            case JavaTypes.OBJECT:
                return getDBDictionary().getBlobObject(_rs, ((Number) obj).intValue(), getStore());
            case JavaTypes.DATE:
                return getDateInternal(obj, joins);
            case JavaTypes.CALENDAR:
                return getCalendarInternal(obj, joins);
            case JavaTypes.BIGDECIMAL:
                return getBigDecimalInternal(obj, joins);
            case JavaTypes.NUMBER:
                return getNumberInternal(obj, joins);
            case JavaTypes.BIGINTEGER:
                return getBigIntegerInternal(obj, joins);
            case JavaTypes.LOCALE:
                return getLocaleInternal(obj, joins);
            case JavaSQLTypes.SQL_ARRAY:
                return getArrayInternal(obj, joins);
            case JavaSQLTypes.ASCII_STREAM:
                return getAsciiStreamInternal(obj, joins);
            case JavaSQLTypes.BINARY_STREAM:
                return getBinaryStreamInternal(obj, joins);
            case JavaSQLTypes.BLOB:
                return getBlobInternal(obj, joins);
            case JavaSQLTypes.BYTES:
                return getBytesInternal(obj, joins);
            case JavaSQLTypes.CHAR_STREAM:
                return getCharacterStreamInternal(obj, joins);
            case JavaSQLTypes.CLOB:
                return getClobInternal(obj, joins);
            case JavaSQLTypes.SQL_DATE:
                return getDateInternal(obj, (Calendar) arg, joins);
            case JavaSQLTypes.SQL_OBJECT:
                return getSQLObjectInternal(obj, (Map) arg, joins);
            case JavaSQLTypes.REF:
                return getRefInternal(obj, (Map) arg, joins);
            case JavaSQLTypes.TIME:
                return getTimeInternal(obj, (Calendar) arg, joins);
            case JavaSQLTypes.TIMESTAMP:
                return getTimestampInternal(obj, (Calendar) arg, joins);
            default:
                if (obj instanceof Column) {
                    Column col = (Column) obj;
                    if (col.getType() == Types.BLOB
                            || col.getType() == Types.VARBINARY) {
                        return getDBDictionary().getBlobObject(_rs, col.getIndex(), getStore());
                    }
                }
                return getDBDictionary().getObject(_rs, ((Number) obj).intValue(), null);
        }
        return (_rs.wasNull()) ? null : val;
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected Object getSQLObjectInternal(Object obj, Map map, Joins joins)
            throws SQLException {
        return getDBDictionary().getObject(_rs, ((Number) obj).intValue(), map);
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected Ref getRefInternal(Object obj, Map map, Joins joins)
            throws SQLException {
        return getDBDictionary().getRef(_rs, ((Number) obj).intValue(), map);
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected short getShortInternal(Object obj, Joins joins)
            throws SQLException {
        return getDBDictionary().getShort(_rs, ((Number) obj).intValue());
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected String getStringInternal(Object obj, Joins joins, boolean isClobString)
            throws SQLException {
        if (isClobString) {
            return getDBDictionary().getClobString(_rs, ((Number) obj).intValue());
        }
        return getDBDictionary().getString(_rs, ((Number) obj).intValue());
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected Time getTimeInternal(Object obj, Calendar cal, Joins joins)
            throws SQLException {
        return getDBDictionary().getTime(_rs, ((Number) obj).intValue(), cal);
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected Timestamp getTimestampInternal(Object obj, Calendar cal,
            Joins joins)
            throws SQLException {
        return getDBDictionary().getTimestamp(_rs, ((Number) obj).intValue(), cal);
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public boolean wasNull()
            throws SQLException {
        return _rs.wasNull();
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected int findObject(Object obj, Joins joins)
            throws SQLException {
        try {
            String s1 = obj.toString();
            DBIdentifier sName = DBIdentifier.newColumn(obj.toString());
            return _rs.findColumn(getDBDictionary().convertSchemaCase(sName));
        } catch (SQLException se) {
            return 0;
        }
    }

    /**
     * {@inheritDoc }
     */
    @Override
    protected InputStream getLOBStreamInternal(JDBCStore store, Object obj,
            Joins joins)
            throws SQLException {
        return getDBDictionary().getLOBStream(store, _rs, ((Number) obj).intValue());
    }
}
