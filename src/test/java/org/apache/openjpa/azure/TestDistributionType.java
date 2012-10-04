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

import java.util.List;
import javax.persistence.EntityManager;
import net.tirasa.jpasqlazure.beans.Gender;
import net.tirasa.jpasqlazure.beans.PersonBIN;
import net.tirasa.jpasqlazure.beans.PersonBINT;
import net.tirasa.jpasqlazure.beans.PersonBIN_PK;
import net.tirasa.jpasqlazure.beans.PersonINT;
import net.tirasa.jpasqlazure.beans.PersonINT_PK;
import net.tirasa.jpasqlazure.beans.PersonUID;
import net.tirasa.jpasqlazure.beans.PersonUID_PK;

public class TestDistributionType extends AbstractAzureTestCase {

    /**
     * Specify persistence unit name as System property
     * <code>-Dunit</code> or use the default value as
     * <code>"azure"</code>.
     */
    @Override
    protected String getPersistenceUnitName() {
        return System.getProperty("unit", "openjpaAzurePersistenceUnit");
    }

    @Override
    public void setUp() {
        super.setUp(
                PersonBINT.class,
                PersonUID.class,
                PersonBIN.class,
                PersonINT.class,
                CLEAR_TABLES);
    }

    public void testBigint() {

        PersonBINT user = new PersonBINT();

        user.setUsername("BobBint");
        user.setPassword("password");
        user.setGender(Gender.M);
        user.setPicture("picture".getBytes());
        user.setInfo("some info");

        final EntityManager entityManager = emf.createEntityManager();

        entityManager.getTransaction().begin();

        entityManager.persist(user);

        List<PersonBINT> res = entityManager.createQuery("SELECT p FROM PersonBINT p").getResultList();

        assertEquals(1, res.size());

        entityManager.remove(res.get(0));

        res = entityManager.createQuery("SELECT p FROM PersonBINT p").getResultList();

        assertTrue(res.isEmpty());

        entityManager.getTransaction().commit();
    }

    public void testInt() {

        PersonINT_PK pk = new PersonINT_PK();
        pk.setCode(1);
        pk.setId(1L);

        PersonINT user = new PersonINT();
        user.setPk(pk);
        user.setUsername("BobInt");
        user.setPassword("password");
        user.setGender(Gender.M);
        user.setPicture("picture".getBytes());
        user.setInfo("some info");

        final EntityManager entityManager = emf.createEntityManager();

        entityManager.getTransaction().begin();

        entityManager.persist(user);

        List<PersonINT> res = entityManager.createQuery("SELECT p FROM PersonINT p").getResultList();

        assertEquals(1, res.size());

        entityManager.remove(res.get(0));

        res = entityManager.createQuery("SELECT p FROM PersonINT p").getResultList();

        assertTrue(res.isEmpty());

        entityManager.getTransaction().commit();
    }

    public void testUniqueidentifier() {

        PersonUID_PK pk = new PersonUID_PK();
        pk.setCode("00000000-0000-0000-0000-000000000002");

        PersonUID user = new PersonUID();
        user.setPk(pk);
        user.setUsername("BobUid");
        user.setPassword("password");
        user.setGender(Gender.M);
        user.setPicture("picture".getBytes());
        user.setInfo("some info");

        final EntityManager entityManager = emf.createEntityManager();

        entityManager.getTransaction().begin();

        entityManager.persist(user);

        List<PersonUID> res = entityManager.createQuery("SELECT p FROM PersonUID p").getResultList();
        assertEquals(1, res.size());

        entityManager.remove(res.get(0));

        res = entityManager.createQuery("SELECT p FROM PersonUID p").getResultList();
        assertTrue(res.isEmpty());

        entityManager.getTransaction().commit();
    }

    public void testVarbinary() {

        PersonBIN_PK pk = new PersonBIN_PK();
        pk.setCode("_!!MRTFBA77L26G141F!!_".getBytes());

        PersonBIN user = new PersonBIN();
        user.setPk(pk);
        user.setUsername("BobBin");
        user.setPassword("password");
        user.setGender(Gender.M);
        user.setPicture("picture".getBytes());
        user.setInfo("some info");

        final EntityManager entityManager = emf.createEntityManager();

        entityManager.getTransaction().begin();

        entityManager.persist(user);

        List<PersonBIN> res = entityManager.createQuery("SELECT p FROM PersonBIN p").getResultList();
        assertEquals(1, res.size());

        entityManager.remove(res.get(0));

        res = entityManager.createQuery("SELECT p FROM PersonBIN p").getResultList();
        assertTrue(res.isEmpty());

        entityManager.getTransaction().commit();
    }
}
