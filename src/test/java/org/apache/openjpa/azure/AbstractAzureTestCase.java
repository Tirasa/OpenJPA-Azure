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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import javax.persistence.EntityManager;
import org.apache.openjpa.azure.beans.PObject;
import org.apache.openjpa.azure.jdbc.conf.AzureConfiguration;
import org.apache.openjpa.persistence.test.SingleEMFTestCase;

public abstract class AbstractAzureTestCase extends SingleEMFTestCase {

    protected List<Class> findClasses(final File directory, final String packageName) {
        final List<Class> classes = new ArrayList<Class>();
        if (!directory.exists()) {
            return classes;
        }
        final File[] files = directory.listFiles();
        for (File file : files) {
            if (file.isDirectory()) {
                assertFalse(file.getName().contains("."));
                classes.addAll(findClasses(file, packageName + "." + file.getName()));
            } else if (file.getName().endsWith(".class")) {
                final String className = packageName + '.' + file.getName().substring(0, file.getName().length() - 6);
                try {
                    classes.add(Class.forName(className));
                } catch (ClassNotFoundException e) {
                    getLog().error("While trying to load class " + className, e);
                }
            }
        }
        return classes;
    }

    protected Class[] getClasses(final String packageName) {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        assertNotNull(classLoader);

        final String path = packageName.replace('.', '/');

        Enumeration<URL> resources = null;
        try {
            resources = classLoader.getResources(path);
        } catch (IOException e) {
            getLog().error("While trying to get resources from " + path, e);
        }
        assertNotNull(resources);

        final List<File> dirs = new ArrayList<File>();
        while (resources.hasMoreElements()) {
            final URL resource = resources.nextElement();
            dirs.add(new File(resource.getFile()));
        }

        final ArrayList<Class> classes = new ArrayList<Class>();
        for (File directory : dirs) {
            classes.addAll(findClasses(directory, packageName));
        }

        return classes.toArray(new Class[classes.size()]);
    }

    @Override
    protected void setUp(final Object... props) {
        super.setUp(props);
        assertTrue(emf.getClass().getName() + " is not a SQL Azure configuration. "
                + "Check that BrokerFactory for the persistence unit is set to azure",
                emf.getConfiguration() instanceof AzureConfiguration);

    }

    @Override
    public void setUp() {
        setUp(getClasses("org.apache.openjpa.azure.beans"), DROP_TABLES);
    }

    @Override
    public int count(final Class<?> type) {
        final EntityManager entityManager = emf.createEntityManager();
        final String query = "SELECT COUNT(p) FROM " + type.getSimpleName() + " p";
        final Number number = (Number) entityManager.createQuery(query).getSingleResult();
        return number.intValue();
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
}
