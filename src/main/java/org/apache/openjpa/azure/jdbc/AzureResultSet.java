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
package org.apache.openjpa.azure.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.Map;

/**
 * A chain of ResultSet.
 *
 * Assumes added ResultSet are identical in structure and fetches forward. Can not move absolutely or change fetch
 * direction.
 */
public class AzureResultSet implements ResultSet {

    private LinkedList<ResultSet> results = new LinkedList<ResultSet>();

    private ResultSet current;

    private int cursor = -1;

    /**
     * Adds the ResultSet only if it has rows.
     */
    public void add(ResultSet rs) {
        results.add(rs);
        if (current == null) {
            current = rs;
            cursor = 0;
        }
    }

    @Override
    public void afterLast()
            throws SQLException {
        current = null;
        cursor = results.size();
    }

    @Override
    public void cancelRowUpdates()
            throws SQLException {
        current.cancelRowUpdates();
    }

    @Override
    public void clearWarnings()
            throws SQLException {
        for (ResultSet rs : results) {
            rs.clearWarnings();
        }
    }

    @Override
    public void close()
            throws SQLException {
        for (ResultSet rs : results) {
            rs.close();
        }
    }

    @Override
    public int findColumn(String name)
            throws SQLException {
        return current.findColumn(name);
    }

    @Override
    public Array getArray(int field)
            throws SQLException {
        return current.getArray(field);
    }

    @Override
    public Array getArray(String field)
            throws SQLException {
        return current.getArray(field);
    }

    @Override
    public InputStream getAsciiStream(int field)
            throws SQLException {
        return current.getAsciiStream(field);
    }

    @Override
    public InputStream getAsciiStream(String field)
            throws SQLException {
        return current.getAsciiStream(field);
    }

    @Override
    public BigDecimal getBigDecimal(int field)
            throws SQLException {
        return current.getBigDecimal(field);
    }

    @Override
    public BigDecimal getBigDecimal(String field)
            throws SQLException {
        return current.getBigDecimal(field);
    }

    @Override
    public BigDecimal getBigDecimal(int field, int arg1)
            throws SQLException {
        return current.getBigDecimal(field, arg1);
    }

    @Override
    public BigDecimal getBigDecimal(String field, int arg1)
            throws SQLException {
        return current.getBigDecimal(field, arg1);
    }

    @Override
    public InputStream getBinaryStream(int field)
            throws SQLException {
        return current.getBinaryStream(field);
    }

    @Override
    public InputStream getBinaryStream(String field)
            throws SQLException {
        return current.getBinaryStream(field);
    }

    @Override
    public Blob getBlob(int field)
            throws SQLException {
        return current.getBlob(field);
    }

    @Override
    public Blob getBlob(String field)
            throws SQLException {
        return current.getBlob(field);
    }

    @Override
    public boolean getBoolean(int field)
            throws SQLException {
        return current.getBoolean(field);
    }

    @Override
    public boolean getBoolean(String field)
            throws SQLException {
        return current.getBoolean(field);
    }

    @Override
    public byte getByte(int field)
            throws SQLException {
        return current.getByte(field);
    }

    @Override
    public byte getByte(String field)
            throws SQLException {
        return current.getByte(field);
    }

    @Override
    public byte[] getBytes(int field)
            throws SQLException {
        return current.getBytes(field);
    }

    @Override
    public byte[] getBytes(String field)
            throws SQLException {
        return current.getBytes(field);
    }

    @Override
    public Reader getCharacterStream(int field)
            throws SQLException {
        return current.getCharacterStream(field);
    }

    @Override
    public Reader getCharacterStream(String field)
            throws SQLException {
        return current.getCharacterStream(field);
    }

    @Override
    public Clob getClob(int field)
            throws SQLException {
        return current.getClob(field);
    }

    @Override
    public Clob getClob(String field)
            throws SQLException {
        return current.getClob(field);
    }

    @Override
    public int getConcurrency()
            throws SQLException {
        return current.getConcurrency();
    }

    @Override
    public String getCursorName()
            throws SQLException {
        return current.getCursorName();
    }

    @Override
    public Date getDate(int field)
            throws SQLException {
        return current.getDate(field);
    }

    @Override
    public Date getDate(String field)
            throws SQLException {
        return current.getDate(field);
    }

    @Override
    public Date getDate(int field, Calendar arg1)
            throws SQLException {
        return current.getDate(field, arg1);
    }

    @Override
    public Date getDate(String field, Calendar arg1)
            throws SQLException {
        return current.getDate(field, arg1);
    }

    @Override
    public double getDouble(int field)
            throws SQLException {
        return current.getDouble(field);
    }

    @Override
    public double getDouble(String field)
            throws SQLException {
        return current.getDouble(field);
    }

    @Override
    public int getFetchDirection()
            throws SQLException {
        return current.getFetchDirection();
    }

    @Override
    public int getFetchSize()
            throws SQLException {
        return current.getFetchSize();
    }

    @Override
    public float getFloat(int field)
            throws SQLException {
        return current.getFloat(field);
    }

    @Override
    public float getFloat(String field)
            throws SQLException {
        return current.getFloat(field);
    }

    @Override
    public int getInt(int field)
            throws SQLException {
        return current.getInt(field);
    }

    @Override
    public int getInt(String field)
            throws SQLException {
        return current.getInt(field);
    }

    @Override
    public long getLong(int field)
            throws SQLException {
        return current.getLong(field);
    }

    @Override
    public long getLong(String field)
            throws SQLException {
        return current.getLong(field);
    }

    @Override
    public ResultSetMetaData getMetaData()
            throws SQLException {
        return current.getMetaData();
    }

    @Override
    public Object getObject(int field)
            throws SQLException {
        return current.getObject(field);
    }

    @Override
    public Object getObject(String field)
            throws SQLException {
        return current.getObject(field);
    }

    @Override
    public Object getObject(int field, Map<String, Class<?>> arg1)
            throws SQLException {
        return current.getObject(field, arg1);
    }

    @Override
    public Object getObject(String field, Map<String, Class<?>> arg1)
            throws SQLException {
        return current.getObject(field, arg1);
    }

    @Override
    public Ref getRef(int field)
            throws SQLException {
        return current.getRef(field);
    }

    @Override
    public Ref getRef(String field)
            throws SQLException {
        return current.getRef(field);
    }

    @Override
    public short getShort(int field)
            throws SQLException {
        return current.getShort(field);
    }

    @Override
    public short getShort(String field)
            throws SQLException {
        return current.getShort(field);
    }

    @Override
    public Statement getStatement()
            throws SQLException {
        return current.getStatement();
    }

    @Override
    public String getString(int field)
            throws SQLException {
        return current.getString(field);
    }

    @Override
    public String getString(String field)
            throws SQLException {
        return current.getString(field);
    }

    @Override
    public Time getTime(int field)
            throws SQLException {
        return current.getTime(field);
    }

    @Override
    public Time getTime(String field)
            throws SQLException {
        return current.getTime(field);
    }

    @Override
    public Time getTime(int field, Calendar arg1)
            throws SQLException {
        return current.getTime(field, arg1);
    }

    @Override
    public Time getTime(String field, Calendar arg1)
            throws SQLException {
        return current.getTime(field, arg1);
    }

    @Override
    public Timestamp getTimestamp(int field)
            throws SQLException {
        return current.getTimestamp(field);
    }

    @Override
    public Timestamp getTimestamp(String field)
            throws SQLException {
        return current.getTimestamp(field);
    }

    @Override
    public Timestamp getTimestamp(int field, Calendar arg1)
            throws SQLException {
        return current.getTimestamp(field, arg1);
    }

    @Override
    public Timestamp getTimestamp(String field, Calendar arg1)
            throws SQLException {
        return current.getTimestamp(field, arg1);
    }

    @Override
    public int getType()
            throws SQLException {
        return current.getType();
    }

    @Override
    public URL getURL(int field)
            throws SQLException {
        return current.getURL(field);
    }

    @Override
    public URL getURL(String field)
            throws SQLException {
        return current.getURL(field);
    }

    @Override
    public InputStream getUnicodeStream(int field)
            throws SQLException {
        return current.getUnicodeStream(field);
    }

    @Override
    public InputStream getUnicodeStream(String field)
            throws SQLException {
        return current.getUnicodeStream(field);
    }

    @Override
    public SQLWarning getWarnings()
            throws SQLException {
        return current.getWarnings();
    }

    @Override
    public boolean isAfterLast()
            throws SQLException {
        return current == null && cursor >= results.size();
    }

    @Override
    public boolean isBeforeFirst()
            throws SQLException {
        return current == null && cursor < 0;
    }

    @Override
    public boolean isFirst()
            throws SQLException {
        return current != null && current.isFirst() && cursor == 0;
    }

    @Override
    public boolean isLast()
            throws SQLException {
        return current != null && current.isLast() && cursor == results.size() - 1;
    }

    @Override
    public boolean last()
            throws SQLException {
        if (results.isEmpty()) {
            return false;
        }
        cursor = results.size() - 1;
        current = results.getLast();
        return current.last();
    }

    @Override
    public boolean next()
            throws SQLException {
        if (results.isEmpty()) {
            return false;
        }
        if (current == null) {
            current = results.get(0);
            cursor = 0;
        }
        if (current.next()) {
            return true;
        }

        cursor++;

        while (cursor < results.size()) {
            current = results.get(cursor);
            if (current.next()) {
                return true;
            }
            cursor++;
        }

        return false;
    }

    @Override
    public boolean relative(int arg0)
            throws SQLException {
        if (arg0 == 0) {
            return current != null;
        }
        boolean forward = arg0 > 0;
        for (int i = 0; i < arg0; i++) {
            if (forward) {
                if (!next()) {
                    return false;
                }
            } else {
                if (!previous()) {
                    return false;
                }
            }
        }
        return false;
    }

    @Override
    public void setFetchSize(int size)
            throws SQLException {
        for (ResultSet rs : results) {
            rs.setFetchSize(size);
        }
    }

    @Override
    public boolean wasNull()
            throws SQLException {
        return current.wasNull();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public <T> T unwrap(Class<T> iface)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getHoldability()
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Reader getNCharacterStream(int arg0)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Reader getNCharacterStream(String arg0)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public NClob getNClob(int arg0)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public NClob getNClob(String arg0)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getNString(int arg0)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getNString(String arg0)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getRow()
            throws SQLException {

        if (current == null) {
            return 0;
        }

        int row = 0;
        for (int i = 0; i < cursor; i++) {
            results.get(i).last();
            row += results.get(i).getRow();
        }
        row += current.getRow();

        return row;
    }

    @Override
    public RowId getRowId(int columnIndex)
            throws SQLException {

        if (current == null) {
            throw new SQLException("current is not set");
        }

        return current.getRowId(columnIndex);
    }

    @Override
    public RowId getRowId(String columnLabel)
            throws SQLException {

        if (current == null) {
            throw new SQLException("current is not set");
        }

        return current.getRowId(columnLabel);
    }

    @Override
    public SQLXML getSQLXML(int arg0)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public SQLXML getSQLXML(String arg0)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isClosed()
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateAsciiStream(int arg0, InputStream arg1)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateAsciiStream(String arg0, InputStream arg1)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateAsciiStream(int arg0, InputStream arg1, long arg2)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateAsciiStream(String arg0, InputStream arg1, long arg2)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBinaryStream(int arg0, InputStream arg1)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBinaryStream(String arg0, InputStream arg1)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBinaryStream(int arg0, InputStream arg1, long arg2)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBinaryStream(String arg0, InputStream arg1, long arg2)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBlob(int arg0, InputStream arg1)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBlob(String arg0, InputStream arg1)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBlob(int arg0, InputStream arg1, long arg2)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBlob(String arg0, InputStream arg1, long arg2)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateCharacterStream(int arg0, Reader arg1)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateCharacterStream(String arg0, Reader arg1)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateCharacterStream(int arg0, Reader arg1, long arg2)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateCharacterStream(String arg0, Reader arg1, long arg2)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateClob(int arg0, Reader arg1)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateClob(String arg0, Reader arg1)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateClob(int arg0, Reader arg1, long arg2)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateClob(String arg0, Reader arg1, long arg2)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateNCharacterStream(int arg0, Reader arg1)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateNCharacterStream(String arg0, Reader arg1)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateNCharacterStream(int arg0, Reader arg1, long arg2)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateNCharacterStream(String arg0, Reader arg1, long arg2)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateNClob(int arg0, NClob arg1)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateNClob(String arg0, NClob arg1)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateNClob(int arg0, Reader arg1)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateNClob(String arg0, Reader arg1)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateNClob(int arg0, Reader arg1, long arg2)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateNClob(String arg0, Reader arg1, long arg2)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateNString(int arg0, String arg1)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateNString(String arg0, String arg1)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateRowId(int arg0, RowId arg1)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateRowId(String arg0, RowId arg1)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateSQLXML(int arg0, SQLXML arg1)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateSQLXML(String arg0, SQLXML arg1)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    // Java 7 methods follow
    public <T> T getObject(String columnLabel, Class<T> type)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public <T> T getObject(int columnIndex, Class<T> type)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void beforeFirst()
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean first()
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean absolute(int i)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean previous()
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setFetchDirection(int i)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean rowUpdated()
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean rowInserted()
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean rowDeleted()
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateNull(int i)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBoolean(int i, boolean bln)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateByte(int i, byte b)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateShort(int i, short s)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateInt(int i, int i1)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateLong(int i, long l)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateFloat(int i, float f)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateDouble(int i, double d)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBigDecimal(int i, BigDecimal bd)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateString(int i, String string)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBytes(int i, byte[] bytes)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateDate(int i, Date date)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateTime(int i, Time time)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateTimestamp(int i, Timestamp tmstmp)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateAsciiStream(int i, InputStream in, int i1)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBinaryStream(int i, InputStream in, int i1)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateCharacterStream(int i, Reader reader, int i1)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateObject(int i, Object o, int i1)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateObject(int i, Object o)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateNull(String string)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBoolean(String string, boolean bln)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateByte(String string, byte b)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateShort(String string, short s)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateInt(String string, int i)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateLong(String string, long l)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateFloat(String string, float f)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateDouble(String string, double d)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBigDecimal(String string, BigDecimal bd)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateString(String string, String string1)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBytes(String string, byte[] bytes)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateDate(String string, Date date)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateTime(String string, Time time)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateTimestamp(String string, Timestamp tmstmp)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateAsciiStream(String string, InputStream in, int i)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBinaryStream(String string, InputStream in, int i)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateCharacterStream(String string, Reader reader, int i)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateObject(String string, Object o, int i)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateObject(String string, Object o)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void insertRow()
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateRow()
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void deleteRow()
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void refreshRow()
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void moveToInsertRow()
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void moveToCurrentRow()
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateRef(int i, Ref ref)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateRef(String string, Ref ref)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBlob(int i, Blob blob)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateBlob(String string, Blob blob)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateClob(int i, Clob clob)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateClob(String string, Clob clob)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateArray(int i, Array array)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateArray(String string, Array array)
            throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public boolean isEmpty() {
        return results.isEmpty();
    }

    public int size() {
        return results.size();
    }
}
