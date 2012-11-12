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
package org.apache.openjpa.slice.jdbc;

import java.sql.SQLException;
import org.apache.openjpa.azure.Federation;
import org.apache.openjpa.azure.util.AzureUtils;
import org.apache.openjpa.slice.Slice;

public class AzureSliceStoreManager extends SliceStoreManager {

    private Federation federation = null;

    public AzureSliceStoreManager(Slice slice) {
        super(slice);
    }

    @Override
    protected RefCountConnection connectInternal()
            throws SQLException {
        final RefCountConnection conn = super.connectInternal();

        // TODO: what about the members?
        if (federation != null) {
            AzureUtils.useFederation(conn, federation);
        }

        return conn;
    }

    void addFederation(final Federation federation) {
        this.federation = federation;
    }
}
