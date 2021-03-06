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
package org.apache.openjpa.azure.jdbc.kernel;

import org.apache.openjpa.azure.jdbc.kernel.exps.AzureJDBCExpressionFactory;
import org.apache.openjpa.jdbc.conf.JDBCConfiguration;
import org.apache.openjpa.jdbc.kernel.JDBCStore;
import org.apache.openjpa.jdbc.kernel.JDBCStoreManager;
import org.apache.openjpa.jdbc.kernel.JDBCStoreQuery;
import org.apache.openjpa.jdbc.meta.ClassMapping;
import org.apache.openjpa.kernel.QueryContext;
import org.apache.openjpa.kernel.exps.ExpressionFactory;
import org.apache.openjpa.kernel.exps.ExpressionParser;
import org.apache.openjpa.meta.ClassMetaData;

public class AzureJDBCStoreQuery extends JDBCStoreQuery {

    public AzureJDBCStoreQuery(JDBCStore store, ExpressionParser parser) {
        super(store, parser);
    }

    @Override
    protected ExpressionFactory getExpressionFactory(ClassMetaData meta) {
        return new AzureJDBCExpressionFactory((ClassMapping) meta);
    }

    @Override
    public void setContext(QueryContext ctx) {
        // Current JDBCStore could refer a closed context.
        // Please, take a look at OPENJPA-2302.
        if (getStore() instanceof JDBCStoreManager && getStore().getContext().getBroker().isClosed()) {
            ((JDBCStoreManager) getStore()).setContext(
                    ctx.getStoreContext(), (JDBCConfiguration) getStore().getConfiguration());
        }

        super.setContext(ctx);
    }
}
