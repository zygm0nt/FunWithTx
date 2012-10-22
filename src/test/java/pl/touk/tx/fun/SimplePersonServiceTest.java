package pl.touk.tx.fun;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import pl.touk.tx.fun.model.Person;

/**
 * @author mcl
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/applicationCtx.xml"})
public class SimplePersonServiceTest {

    private Logger log = Logger.getLogger(SimplePersonServiceTest.class);

    @Autowired
    SimpleDao dao;

    @Autowired
    SimplePersonService service;

    @Before
    public void setup() throws Exception {
        dao.update("drop table if exists persons");
        dao.update("create table persons (name varchar, age integer)");
    }

    @Test
    public void shouldAddPerson() throws Exception {
        Assert.assertEquals(0, service.fetch().size());
        service.persist(new Person("John", 25));
        Assert.assertEquals(1, service.fetch().size());
        try {
            service.persistWithException(new Person("Tom", 11));
        } catch (RuntimeException e) {
            log.warn("Got RuntimeException");
        }
        Assert.assertEquals(1, service.fetch().size());
    }

}
