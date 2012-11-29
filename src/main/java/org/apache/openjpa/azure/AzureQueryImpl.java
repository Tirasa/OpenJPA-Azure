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
package org.apache.openjpa.azure;

import java.util.Collections;
import java.util.Map;

import org.apache.openjpa.kernel.Broker;
import org.apache.openjpa.kernel.QueryImpl;
import org.apache.openjpa.kernel.StoreQuery;
import org.apache.openjpa.slice.DistributedConfiguration;
import org.apache.openjpa.slice.QueryTargetPolicy;
import org.apache.openjpa.slice.ReentrantSliceLock;
import org.apache.openjpa.slice.jdbc.TargetFetchConfiguration;

/**
 * Extension with slice locking policy.
 *
 * @author Pinaki Poddar
 *
 */
@SuppressWarnings("serial")
public class AzureQueryImpl extends QueryImpl {

    private final ReentrantSliceLock _lock;

    private DistributedConfiguration _conf;

    private final Broker broker;

    public AzureQueryImpl(Broker broker, String language, StoreQuery storeQuery) {
        super(broker, language, storeQuery);
        this.broker = broker;
        _lock = new ReentrantSliceLock();
        _conf = (DistributedConfiguration) broker.getConfiguration();
    }

    /**
     * Overrides to set the query targets via policy, if user has not already set the targets via hint explicitly on
     * this query.
     */
    @Override
    public Object execute(Map params) {
        TargetFetchConfiguration fetch = (TargetFetchConfiguration) getFetchConfiguration();
        if (!fetch.isExplicitTarget()) {
            QueryTargetPolicy policy = _conf.getQueryTargetPolicyInstance();
            if (policy != null) {
                String[] targets = policy.getTargets(getQueryString(), Collections.unmodifiableMap(params),
                        getLanguage(), _conf.getActiveSliceNames(), this.getBroker());
                fetch.setTargets(targets);
            }
        }

        return super.execute(params);
    }

    public long deleteAll(Object[] params) {
        TargetFetchConfiguration fetch = (TargetFetchConfiguration) getFetchConfiguration();
        if (!fetch.isExplicitTarget()) {
            QueryTargetPolicy policy = _conf.getQueryTargetPolicyInstance();

            if (policy != null) {
                String[] targets = policy.getTargets(getQueryString(), null,
                        getLanguage(), _conf.getActiveSliceNames(), this.getBroker());

                fetch.setTargets(targets);
            }
        }
        return super.deleteAll(params);
    }
    
    public long deleteAll(Map params) {
        TargetFetchConfiguration fetch = (TargetFetchConfiguration) getFetchConfiguration();
        if (!fetch.isExplicitTarget()) {
            QueryTargetPolicy policy = _conf.getQueryTargetPolicyInstance();

            if (policy != null) {
                String[] targets = policy.getTargets(getQueryString(), null,
                        getLanguage(), _conf.getActiveSliceNames(), this.getBroker());

                fetch.setTargets(targets);
            }
        }
        return super.deleteAll(params);
    }

    public long updateAll(Object[] params) {
        TargetFetchConfiguration fetch = (TargetFetchConfiguration) getFetchConfiguration();
        if (!fetch.isExplicitTarget()) {
            QueryTargetPolicy policy = _conf.getQueryTargetPolicyInstance();

            if (policy != null) {
                String[] targets = policy.getTargets(getQueryString(), null,
                        getLanguage(), _conf.getActiveSliceNames(), this.getBroker());

                fetch.setTargets(targets);
            }
        }
        return super.updateAll(params);
    }

    @Override
    public long updateAll(Map params) {
        TargetFetchConfiguration fetch = (TargetFetchConfiguration) getFetchConfiguration();
        if (!fetch.isExplicitTarget()) {
            QueryTargetPolicy policy = _conf.getQueryTargetPolicyInstance();

            if (policy != null) {
                String[] targets = policy.getTargets(getQueryString(), null,
                        getLanguage(), _conf.getActiveSliceNames(), this.getBroker());

                fetch.setTargets(targets);
            }
        }
        return super.updateAll(params);
    }

    /**
     * Always uses lock irrespective of super's multi-threaded settings.
     */
    @Override
    public void lock() {
        _lock.lock();
    }

    @Override
    public void unlock() {
        _lock.unlock();
    }
}
