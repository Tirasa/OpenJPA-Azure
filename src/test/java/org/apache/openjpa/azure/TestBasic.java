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

import java.util.ArrayList;
import java.util.List;
import javax.persistence.EntityManager;

public class TestBasic extends AbstractAzureTestCase {

    /**
     * Specify persistence unit name as System property
     * <code>-Dunit</code> or use the default value as
     * <code>"azure"</code>.
     */
    @Override
    protected String getPersistenceUnitName() {
        return System.getProperty("unit", "azure");
    }

    @Override
    public void setUp() {
        super.setUp(PObject.class, Person.class, Address.class, Country.class, CLEAR_TABLES);
    }

    /**
     * Persist num independent objects.
     */
    protected List<PObject> createIndependentObjects(final int num) {
        final List<PObject> pcs = new ArrayList<PObject>();
        final EntityManager entityManager = emf.createEntityManager();
        entityManager.getTransaction().begin();
        for (int i = 0; i < num; i++) {
            final PObject pobj = new PObject();
            pcs.add(pobj);
            entityManager.persist(pobj);
            pobj.setValue(10 + i);
        }
        entityManager.getTransaction().commit();
        entityManager.clear();
        return pcs;
    }

    /**
     * Create a single object.
     */
    protected PObject createIndependentObject() {
        return createIndependentObjects(1).get(0);
    }

    /**
     * Delete a single object by EntityManager.remove().
     */
    public void testDelete() {
        createIndependentObjects(10);

        final EntityManager entityManager = emf.createEntityManager();

        entityManager.getTransaction().begin();
        final int before = count(PObject.class);
        System.out.println("MMMMMMMMMMMMMM0 ");
        final List<PObject> all = entityManager.createQuery("SELECT p FROM PObject p").getResultList();
        System.out.println("MMMMMMMMMMMMMMM " + all);
        assertFalse(all.isEmpty());
        entityManager.remove(all.get(0));
        entityManager.getTransaction().commit();

        assertEquals(before - 1, count(PObject.class));
    }

    /**
     * Delete in bulk by query.
     */
    public void NOTtestBulkDelete() {
        final EntityManager entityManager = emf.createEntityManager();

        entityManager.getTransaction().begin();
        final int count = count(PObject.class);
        final int deleted = entityManager.createQuery("DELETE FROM PObject p").executeUpdate();
        assertEquals(count, deleted);
        entityManager.getTransaction().commit();

        assertEquals(0, count(PObject.class));
    }
}
