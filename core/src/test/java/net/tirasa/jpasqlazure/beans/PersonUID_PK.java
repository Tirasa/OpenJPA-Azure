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
import javax.persistence.Lob;
import org.apache.commons.lang.ArrayUtils;

@Embeddable
public class PersonUID_PK implements Serializable {

    @Lob
    @Column(nullable = false, columnDefinition = "uniqueidentifier")
    private String code;

//    public byte[] getCode() {
//        return ArrayUtils.clone(code);
//    }
//
//    public void setCode(byte[] code) {
//        this.code = ArrayUtils.clone(code);
//    }
    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    @Override
    public boolean equals(Object o) {
        return o != null && o instanceof PersonUID_PK
                && ArrayUtils.isEquals(this.getCode(), ((PersonUID_PK) o).getCode());
    }

    @Override
    public int hashCode() {
        int hash = 9;
        hash = (31 * hash) + (null == code ? 0 : code.hashCode());
        return hash;
    }
}
