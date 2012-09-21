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

import java.io.Serializable;
import javax.persistence.Column;
import javax.persistence.Embeddable;

@Embeddable
public class PersonINT_PK implements Serializable {

    @Column(nullable = false)
    private Long id;

    @Column(nullable = false, columnDefinition = "int")
    private Integer code;

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        return o != null && o instanceof PersonINT_PK
                && this.getId().equals(((PersonINT_PK) o).getId())
                && this.getCode().equals(((PersonINT_PK) o).getCode());
    }

    @Override
    public int hashCode() {
        int hash = 9;
        hash = (31 * hash) + (null == id ? 0 : id.hashCode());
        hash = (31 * hash) + (null == code ? 0 : code.hashCode());
        return hash;
    }
}
