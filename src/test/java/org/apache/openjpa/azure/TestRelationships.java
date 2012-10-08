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
package org.apache.openjpa.azure;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.Query;
import org.apache.openjpa.azure.beans.BusinessRole;
import org.apache.openjpa.azure.beans.Gender;
import org.apache.openjpa.azure.beans.PersonBINT;

public class TestRelationships extends AbstractAzureTestCase {

    @Override
    protected String getPersistenceUnitName() {
        return System.getProperty("unit", "azure-test");
    }

    public void testJoinTable()
            throws UnsupportedEncodingException {

        PersonBINT user = new PersonBINT();

        user.setUsername("BobBint");
        user.setPassword("password");
        user.setGender(Gender.M);
        user.setPicture("picture".getBytes());
        user.setInfo("some info");

        final EntityManager entityManager = emf.createEntityManager();

        entityManager.getTransaction().begin();

        user = entityManager.merge(user);

        BusinessRole br = new BusinessRole();
        br.setName("roleA");

        br = entityManager.merge(br);

        user.setRoles(new HashSet<BusinessRole>(Collections.singleton(br)));

        user = entityManager.merge(user);

        entityManager.getTransaction().commit();

        user = entityManager.find(PersonBINT.class, user.getId());
        assertNotNull(user);
        assertEquals(1, user.getRoles().size());

        br = entityManager.find(BusinessRole.class, user.getRoles().iterator().next().getId());
        assertNotNull(br);

        Query query = entityManager.createNativeQuery("SELECT * FROM Membership WHERE personId = " + user.getId());
        List res = query.getResultList();
        assertEquals(1, res.size());

        entityManager.getTransaction().begin();

        entityManager.remove(user);
        entityManager.remove(br);

        entityManager.getTransaction().commit();

        query = entityManager.createNativeQuery("SELECT * FROM Membership WHERE personId = " + user.getId());
        res = query.getResultList();
        assertTrue(res.isEmpty());
    }
}
