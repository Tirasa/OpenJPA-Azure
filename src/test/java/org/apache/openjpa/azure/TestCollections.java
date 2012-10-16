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

import java.util.Collections;
import java.util.List;
import javax.persistence.EntityManager;
import org.apache.openjpa.azure.beans.Gender;
import org.apache.openjpa.azure.beans.PersonBINT;

public class TestCollections extends AbstractAzureTestCase {

    @Override
    protected String getPersistenceUnitName() {
        return System.getProperty("unit", "azure-test");
    }

    public void testFake() {
        assertTrue(true);
    }

    /**
     * JOIN implementatin required .....
     */
    public void NOTtestEmptySet() {
        final EntityManager entityManager = emf.createEntityManager();

        PersonBINT user1 = new PersonBINT();

        user1.setUsername("BobBint1");
        user1.setPassword("password");
        user1.setGender(Gender.M);
        user1.setPicture("picture".getBytes());
        user1.setInfo("some info");

        entityManager.getTransaction().begin();

        user1 = entityManager.merge(user1);

        entityManager.getTransaction().commit();

        PersonBINT user2 = new PersonBINT();

        user2.setUsername("BobBint2");
        user2.setPassword("password");
        user2.setGender(Gender.M);
        user2.setPicture("picture".getBytes());
        user2.setInfo("some info");
        user2.setNickNames(Collections.singleton("bobbint"));

        entityManager.getTransaction().begin();

        user2 = entityManager.merge(user2);

        entityManager.getTransaction().commit();

        List<PersonBINT> res = entityManager.createQuery(
                "SELECT e FROM PersonBINT e WHERE e.nickNames IS NOT EMPTY", PersonBINT.class).getResultList();

        assertEquals(1, res.size());
        assertEquals("BobBint2", res.get(0).getUsername());
    }

    /**
     * JOIN implementatin required .....
     */
    public void NOTtestMemberOf() {
        final EntityManager entityManager = emf.createEntityManager();

        PersonBINT user1 = new PersonBINT();

        user1.setUsername("BobBint3");
        user1.setPassword("password");
        user1.setGender(Gender.M);
        user1.setPicture("picture".getBytes());
        user1.setInfo("some info");

        entityManager.getTransaction().begin();

        user1 = entityManager.merge(user1);

        entityManager.getTransaction().commit();

        PersonBINT user2 = new PersonBINT();

        user2.setUsername("BobBint4");
        user2.setPassword("password");
        user2.setGender(Gender.M);
        user2.setPicture("picture".getBytes());
        user2.setInfo("some info");
        user2.setNickNames(Collections.singleton("bobbint4"));

        entityManager.getTransaction().begin();

        user2 = entityManager.merge(user2);

        entityManager.getTransaction().commit();

        List<PersonBINT> res = entityManager.createQuery(
                "SELECT e FROM PersonBINT e WHERE 'bobbint4' MEMBER OF e.nickNames", PersonBINT.class).getResultList();

        assertEquals(1, res.size());
        assertEquals("BobBint4", res.get(0).getUsername());
    }
}
