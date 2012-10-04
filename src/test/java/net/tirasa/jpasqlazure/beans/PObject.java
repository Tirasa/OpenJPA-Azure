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
package net.tirasa.jpasqlazure.beans;

import java.util.concurrent.atomic.AtomicLong;
import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class PObject {

    @Id
    private long id;

    private int value;

    private static AtomicLong idCounter = new AtomicLong(System.currentTimeMillis());

    public PObject() {
        id = idCounter.addAndGet(1);
    }

    public long getId() {
        return id;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int i) {
        value = i;
    }
}