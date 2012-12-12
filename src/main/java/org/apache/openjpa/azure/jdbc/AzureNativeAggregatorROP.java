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
package org.apache.openjpa.azure.jdbc;

import org.apache.openjpa.kernel.StoreQuery;
import org.apache.openjpa.kernel.exps.QueryExpressions;
import org.apache.openjpa.lib.rop.ResultObjectProvider;

public class AzureNativeAggregatorROP extends AzureUniqueResultObjectProvider {

    public AzureNativeAggregatorROP(
            final ResultObjectProvider[] rops, final StoreQuery query, final QueryExpressions[] exps) {
        super(rops, query, exps);
    }

    @Override
    public boolean next()
            throws Exception {
        if (!_opened) {
            open();
        }

        if (_single != null) {
            return false;
        }

        Object single = null;

        for (ResultObjectProvider rop : _rops) {
            if (rop.next()) {

                Integer row = (Integer) rop.getResultObject();

                single = count(single, row);
            }
        }

        _single = single;
        return true;
    }
}
