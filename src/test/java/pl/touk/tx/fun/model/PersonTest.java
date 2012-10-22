package pl.touk.tx.fun.model;

import org.junit.Test;
import pl.touk.tx.fun.model.Person;

import static org.junit.Assert.*;

public class PersonTest {
    @Test
    public void canConstructAPersonWithAName() {
        Person person = new Person("Larry", 5);
        assertEquals("Larry", person.getName());
    }
}
