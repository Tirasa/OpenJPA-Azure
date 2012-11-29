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

import java.beans.PropertyDescriptor;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.persistence.Embeddable;
import org.apache.commons.lang.StringUtils;
import org.apache.openjpa.azure.Federation;
import org.apache.openjpa.azure.jdbc.conf.AzureConfiguration;
import org.apache.openjpa.azure.jdbc.conf.AzureConfiguration.RangeType;
import org.apache.openjpa.jdbc.meta.MappingRepository;
import org.apache.openjpa.jdbc.schema.ForeignKey;
import org.apache.openjpa.jdbc.schema.Table;
import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.azure.jdbc.AzureSliceStoreManager;
import org.apache.openjpa.slice.jdbc.DistributedJDBCStoreManager;
import org.apache.openjpa.slice.jdbc.SliceStoreManager;
import org.apache.openjpa.util.ObjectId;

public final class AzureUtils {

    private AzureUtils() {
    }

    public static Connection useFederation(final Connection conn, final Federation federation)
            throws SQLException {

        final MemberDistribution memberDistribution = getMemberDistribution(conn, federation);
        return useFederation(conn, federation, memberDistribution.iterator().next());
    }

    public static Connection useFederation(final Connection conn, final Federation federation, final Object oid)
            throws SQLException {

        final String distribution = RangeType.UNIQUEIDENTIFIER == federation.getRangeMappingType()
                ? getUidAsString(oid) : getObjectIdAsString(oid);

        final String distributionName = federation.getDistributionName();

        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute("USE FEDERATION " + federation + " (" + distributionName + " = " + distribution + ") "
                    + "WITH FILTERING=OFF, RESET");
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ignore) {
                    // ignore exception
                }
            }
        }

        return conn;
    }

    public static boolean checkForFederationMember(final Federation federation, final Object member, Object oid) {

        if (federation == null || member == null) {
            return false;
        }

        final RangeType type = federation.getRangeMappingType();

        if (RangeType.UNIQUEIDENTIFIER == type) {
            final String left, right;

            if (oid instanceof byte[]) {
                left = new String((byte[]) oid);
                right = new String((byte[]) member);
            } else {
                left = oid.toString();
                right = member.toString();
                return left.compareTo(right) >= 0;
            }

            return left.compareTo(right) >= 0;
        } else {
            if (oid instanceof byte[]) {
                final String left = "0x" + new String(HexEncoderDecoder.encode((byte[]) oid));
                final String right = "0x" + new String(HexEncoderDecoder.encode((byte[]) member));
                return left.compareTo(right) >= 0;
            } else {
                return Long.parseLong(oid.toString()) >= Long.parseLong(member.toString());
            }
        }
    }

    public static Object getMemberDistribution(final Connection conn, final Federation federation, final Object oid)
            throws SQLException {

        final RangeType type = federation.getRangeMappingType();

        final String rangeId = RangeType.UNIQUEIDENTIFIER == type
                ? "CAST(" + getUidAsString(oid) + " AS UNIQUEIDENTIFIER)" : getObjectIdAsString(oid);

        Statement stm = null;
        ResultSet federation_id = null;
        ResultSet member_distribution = null;

        Object res = null;

        try {
            stm = conn.createStatement();

            federation_id = stm.executeQuery(
                    "SELECT * "
                    + "FROM sys.Federations "
                    + "WHERE name = '" + federation.getName() + "'");

            if (federation_id.next()) {
                member_distribution = stm.executeQuery(
                        "SELECT CAST(MAX(range_low) as " + type.getValue()
                        + ") FROM sys.federation_member_distributions "
                        + "WHERE federation_id=" + federation_id.getInt(1) + " and range_low<=" + rangeId);

                if (member_distribution.next()) {
                    res = member_distribution.getObject(1);
                }
            }

        } finally {
            if (stm != null) {
                stm.close();
            }

            if (federation_id != null) {
                federation_id.close();
            }

            if (member_distribution != null) {
                member_distribution.close();
            }
        }

        return res;
    }

    public static MemberDistribution getMemberDistribution(final Connection conn, final Federation federation)
            throws SQLException {

        final RangeType type = federation.getRangeMappingType();
        final MemberDistribution memberDistribution = new MemberDistribution(type);

        Statement stm = null;
        ResultSet federation_id = null;
        ResultSet member_distribution = null;
        try {
            stm = conn.createStatement();

            federation_id = stm.executeQuery(
                    "SELECT * "
                    + "FROM sys.Federations "
                    + "WHERE name = '" + federation.getName() + "'");

            if (federation_id.next()) {
                member_distribution = stm.executeQuery(
                        "SELECT CAST(range_low as " + type.getValue() + ") AS low "
                        + "FROM sys.federation_member_distributions "
                        + "WHERE federation_id=" + federation_id.getInt(1) + " ORDER BY low");

                while (member_distribution.next()) {
                    memberDistribution.addValue(member_distribution.getObject(1));
                }
            }

        } finally {
            if (stm != null) {
                stm.close();
            }
            if (federation_id != null) {
                federation_id.close();
            }
            if (member_distribution != null) {
                member_distribution.close();
            }
        }

        return memberDistribution;
    }

    /**
     * Check if table exist.
     *
     * @param conn given connection.
     * @param table table to be verified.
     * @param oid range id.
     * @return TRUE if exists; FALSE otherwise.
     * @throws SQLException
     */
    public static boolean tableExists(final Connection conn, final Table table)
            throws SQLException {

        boolean res = false;

        Statement stm = null;
        ResultSet rs = null;
        try {
            stm = conn.createStatement();

            rs = stm.executeQuery(
                    "SELECT DISTINCT OBJECT_NAME (object_id) AS[ObjectName] "
                    + "FROM sys.dm_db_partition_stats "
                    + "WHERE OBJECT_NAME(object_id) ='" + table + "'");

            if (rs.next()) {
                res = true;
            }
        } finally {
            if (rs != null) {
                rs.close();
            }

            if (stm != null) {
                stm.close();
            }
        }

        return res;
    }

    public static Set<Federation> getTargetFederation(
            final AzureConfiguration conf, final Table table, Set<String> tablesToBeExcluded) {

        if (tablesToBeExcluded == null) {
            tablesToBeExcluded = new HashSet<String>();
        }

        tablesToBeExcluded.add(table.getFullIdentifier().getName());

        final Set<Federation> federations = new HashSet<Federation>(conf.getFederations(table));

        final ForeignKey[] fks = table.getForeignKeys();

        for (ForeignKey fk : fks) {
            final Table extTable = fk.getPrimaryKeyTable();
            if (!tablesToBeExcluded.contains(extTable.getFullIdentifier().getName())) {
                federations.addAll(getTargetFederation(conf, extTable, tablesToBeExcluded));
            }
        }

        return federations;
    }

    public static String getTableName(final AzureConfiguration conf, final ClassMetaData meta) {
        return meta == null ? null : getTableName(conf, meta.getDescribedType());
    }

    public static Table getTable(final AzureConfiguration conf, final ClassMetaData meta) {
        return meta == null ? null : getTable(conf, meta.getDescribedType());
    }

    public static String getTableName(final AzureConfiguration conf, final Class describedType) {
        try {
            final Table table = getTable(conf, describedType);
            return table == null ? null : table.getFullIdentifier().getName();
        } catch (Exception e) {
            // ignore exception and search for table by using all the connections
            return null;
        }
    }

    public static Table getTable(final AzureConfiguration conf, final Class describedType) {
        try {
            final MappingRepository repo = conf.getMappingRepositoryInstance();
            final Table table = repo.getMapping(describedType, conf.getClass().getClassLoader(), true).getTable();
            return table;
        } catch (Exception e) {
            // ignore exception and search for table by using all the connections
            return null;
        }
    }

    public static Map.Entry<String, Object> getTargetId(final AzureConfiguration conf, final Class candidate) {
        String tableName = getTableName(conf, candidate);

        // TODO: improve looking for distribution value.
        return new AbstractMap.SimpleEntry<String, Object>(tableName, null);
    }

    public static Map.Entry<String, Object> getTargetId(final AzureConfiguration conf, final ClassMetaData meta) {
        String tableName = getTableName(conf, meta);

        // TODO: improve looking for distribution value.
        return new AbstractMap.SimpleEntry<String, Object>(tableName, null);
    }

    public static String getObjectIdAsString(final Object oid) {
        return oid instanceof byte[] ? "0x" + new String(HexEncoderDecoder.encode((byte[]) oid)) : oid.toString();
    }

    private static String getUidAsString(final Object oid) {
        return oid instanceof byte[] ? new String((byte[]) oid) : "'" + oid.toString() + "'";
    }

    public static Object getObjectId(final Federation fed, final OpenJPAStateManager sm, final String dn) {
        return StringUtils.isNotBlank(dn) ? getObjectIdValue(sm.getObjectId(), dn) : null;
    }

    public static Object getObjectIdValue(final Object oid, final String key) {
        Object value = null;

        if (StringUtils.isNotBlank(key)) {
            try {
                if (oid instanceof ObjectId) {
                    final Object idObject = ((ObjectId) oid).getIdObject();
                    value = new PropertyDescriptor(key, idObject.getClass()).getReadMethod().invoke(idObject);
                } else {
                    final Embeddable embeddable = oid.getClass().getAnnotation(Embeddable.class);
                    if (embeddable == null) {
                        value = oid;
                    } else {
                        value = new PropertyDescriptor(key, oid.getClass()).getReadMethod().invoke(oid);
                    }
                }
            } catch (Exception ignore) {
                // ignore
            }
        }

        return value;
    }

    public static List<String> getTargetSlice(
            final DistributedJDBCStoreManager store, final List<String> slices, final Federation fed, final Object id) {

        final List<String> res = new ArrayList<String>();

        for (int i = slices.size() - 1; i >= 0; i--) {

            final SliceStoreManager sliceStore = store.getSlice(i);

            final Object fedUpperBound = ((AzureSliceStoreManager) sliceStore).getFedUpperBound();
            final boolean isSingleMember = !((AzureSliceStoreManager) sliceStore).isFedMultiMember();

            if (fed.getName().equals(((AzureSliceStoreManager) sliceStore).getFedName())
                    && (id == null
                    || isSingleMember
                    || AzureUtils.checkForFederationMember(fed, fedUpperBound, id))) {
                res.add(sliceStore.getName());
            }
        }

        if (res.isEmpty()) {
            res.add("ROOT");
        }

        return res;
    }

    public static String getFederationName(final String sliceName) {
        int index = sliceName.lastIndexOf(".");

        return index < 0 ? sliceName : sliceName.substring(0, index);
    }

    public static int getSliceMemberIndex(final String sliceName) {
        int index = sliceName.lastIndexOf(".");

        return index < 0 ? 0 : Integer.parseInt(sliceName.substring(index + 1, sliceName.length()));
    }

    public static String getSliceName(final String openjpaId) {
        int index = openjpaId.indexOf(".");

        return index < 0 ? openjpaId : openjpaId.substring(index + 1, openjpaId.length());
    }
}
