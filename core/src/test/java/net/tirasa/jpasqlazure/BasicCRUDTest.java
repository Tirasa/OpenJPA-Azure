/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.tirasa.jpasqlazure;

import java.io.UnsupportedEncodingException;
import net.tirasa.jpasqlazure.beans.Gender;
import net.tirasa.jpasqlazure.beans.Person;
import net.tirasa.jpasqlazure.beans.PersonBIN;
import net.tirasa.jpasqlazure.beans.PersonBIN_PK;
import net.tirasa.jpasqlazure.beans.PersonINT;
import net.tirasa.jpasqlazure.beans.PersonINT_PK;
import net.tirasa.jpasqlazure.beans.PersonUID;
import net.tirasa.jpasqlazure.beans.PersonUID_PK;
import net.tirasa.jpasqlazure.repository.PersonBinRepository;
import net.tirasa.jpasqlazure.repository.PersonIntRepository;
import net.tirasa.jpasqlazure.repository.PersonRepository;
import net.tirasa.jpasqlazure.repository.PersonUidRepository;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
    "classpath:applicationContext.xml"
})
public class BasicCRUDTest {

    private static int USER_NUMBER = 3;

    private static PersonRepository repository = null;

    private static PersonIntRepository repositoryINT = null;

    private static PersonUidRepository repositoryUID = null;

    private static PersonBinRepository repositoryBIN = null;

    private static String[] schemaPurgeQueries = {
        "USE FEDERATION ROOT WITH RESET",
        "drop table OPENJPA_SEQUENCE_TABLE",
        "drop table BusinessRole_Person",
        "drop table BusinessRole",
        "USE FEDERATION FED_1 (range_id=0) WITH FILTERING=OFF, RESET",
        "drop table Person",
        "USE FEDERATION FED_1 (range_id=5) WITH FILTERING=OFF, RESET",
        "drop table Person",
        "USE FEDERATION FED_2 (range_id = '00000000-0000-0000-0000-000000000000') WITH RESET, FILTERING = OFF",
        "drop table PersonUID",
        "USE FEDERATION FED_3 (range_id = 0) WITH RESET, FILTERING = OFF",
        "drop table PersonINT",
        "USE FEDERATION FED_3 (range_id = 5) WITH RESET, FILTERING = OFF",
        "drop table PersonINT",
        "USE FEDERATION FED_4 (range_id = 0) WITH RESET, FILTERING = OFF",
        "drop table PersonBIN"
    };

    @BeforeClass
    public static void init()
            throws UnsupportedEncodingException {
        final ApplicationContext ctx = new ClassPathXmlApplicationContext("/applicationContext.xml");

        repository = ctx.getBean(PersonRepository.class);
        repositoryINT = ctx.getBean(PersonIntRepository.class);
        repositoryUID = ctx.getBean(PersonUidRepository.class);
        repositoryBIN = ctx.getBean(PersonBinRepository.class);

        for (int i = 0; i < USER_NUMBER; i++) {
            Person user = new Person();
            user.setUsername("Bob_" + i);
            user.setPassword("password");
            user.setGender(Gender.values()[i % 2]);
            user.setPicture("picture".getBytes());
            user.setInfo("some info");

            user = repository.save(user);
            assertNotNull(user);
            assertEquals("picture", new String(user.getPicture(), "UTF-8"));
        }
    }

    @Test
    public void bigintTest()
            throws UnsupportedEncodingException {

        Person user = repository.findByUsername("Bob_2");
        assertNotNull(user);

        user.setInfo("other info");
        user = repository.save(user);
        assertNotNull(user);
        assertEquals("other info", user.getInfo());

        Iterable<Person> iter = repository.findAll();
        assertTrue(iter.iterator().hasNext());

        for (Person person : iter) {
            assertNotNull(person);
        }
    }

    @Test
    public void uniqueidentifierTest()
            throws UnsupportedEncodingException {
        PersonUID_PK pk = new PersonUID_PK();
        pk.setCode("00000000-0000-0000-0000-000000000002");

        PersonUID user = new PersonUID();
        user.setPk(pk);
        user.setUsername("BobInt");
        user.setPassword("password");
        user.setGender(Gender.M);
        user.setPicture("picture".getBytes());
        user.setInfo("some info");

        user = repositoryUID.save(user);
        assertNotNull(user);

        user = repositoryUID.findOne(user.getPk());
        assertNotNull(user);

        assertEquals("00000000-0000-0000-0000-000000000002", user.getPk().getCode());

        repositoryUID.delete(user);

        user = repositoryUID.findOne(user.getPk());
        assertNull(user);
    }

    @Test
    public void varbinaryTest()
            throws UnsupportedEncodingException {
        PersonBIN_PK pk = new PersonBIN_PK();
        pk.setCode("_!!MRTFBA77L26G141F!!_".getBytes());

        PersonBIN user = new PersonBIN();
        user.setPk(pk);
        user.setUsername("BobInt");
        user.setPassword("password");
        user.setGender(Gender.M);
        user.setPicture("picture".getBytes());
        user.setInfo("some info");

        user = repositoryBIN.save(user);
        assertNotNull(user);

        user = repositoryBIN.findOne(user.getPk());
        assertNotNull(user);

        repositoryBIN.delete(user);

        user = repositoryBIN.findOne(user.getPk());
        assertNull(user);
    }

    @Test
    public void intTest()
            throws UnsupportedEncodingException {

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

        user = repositoryINT.save(user);
        assertNotNull(user);

        user = repositoryINT.findOne(user.getPk());
        assertNotNull(user);

        repositoryINT.delete(user);

        user = repositoryINT.findOne(user.getPk());
        assertNull(user);
    }

    @Test
    @Ignore
    public void count() {
        assertTrue(repository.count() > 0);
    }

    @AfterClass
    public static void clean() {
        for (int i = 0; i < USER_NUMBER; i++) {
            Person user = repository.findByUsername("Bob_" + i);
            assertNotNull(user);

            repository.delete(user);

            user = repository.findOne(user.getId());
            assertNull(user);
        }

        Initialize.executeQueries(schemaPurgeQueries);
    }
}
