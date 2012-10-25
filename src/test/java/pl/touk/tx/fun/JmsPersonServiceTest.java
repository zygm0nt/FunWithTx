package pl.touk.tx.fun;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import pl.touk.tx.fun.handler.MessageHandler;
import pl.touk.tx.fun.jms.CustomMessageListener;
import pl.touk.tx.fun.jms.SampleMessageProducer;
import pl.touk.tx.fun.model.Person;

import javax.jms.Message;
import javax.jms.TextMessage;

/**
 * @author mcl
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/applicationCtx-withAMQ.xml"})
public class JmsPersonServiceTest {

    private Logger log = Logger.getLogger(JmsPersonServiceTest.class);

    @Autowired SimpleDao dao;

    @Autowired SimplePersonService service;
    @Autowired CustomMessageListener messageListener;
    @Autowired SampleMessageProducer messageProducer;

    @Before
    public void setup() throws Exception {
        dao.update("drop table if exists persons");
        dao.update("create table persons (name varchar, age integer)");
        messageListener.reset();
    }

    @Test
    public void shouldAddPersonFromJMS() throws Exception {
        messageListener.setMessageHandler(new MessageHandler() {
            @Override
            public void handle(Message msg) throws Exception {
                if (msg instanceof TextMessage) {
                    TextMessage txt = (TextMessage) msg;
                    service.persist(new Person(txt.getText(), 1));
                }
            }
        });
        messageProducer.generateMessages();
        Thread.sleep(1000);
        Assert.assertEquals(10, service.fetch().size());
        Assert.assertEquals(10, messageListener.getMessageCounter());
    }

    /**
     * We consume messages from the queue event thou there has been an exception.
     * 
     * Messages are removed from the broker and can be lost due to errors on listener's side.
     *
     * @throws Exception
     */
    @Test
    public void shouldHandleMessageFromJMSWithException() throws Exception {
        messageListener.setMessageHandler(new MessageHandler() {
            @Override
            public void handle(Message msg) throws Exception {
                throw new Exception("Message processing exception");
            }
        });
        messageProducer.generateMessages();
        Thread.sleep(1000);
        Assert.assertEquals(0, service.fetch().size());
        Assert.assertEquals(10, messageListener.getMessageCounter());
    }

    /**
     * Even thou we call <i>persistWithException</i> - which has @Transactional annotation, it does not
     * stop the listener from removing message from JMS queue.
     *
     * @throws Exception
     */
    @Test
    public void shouldHandleMessagesWithTransactionalMethod() throws Exception {
        messageListener.setMessageHandler(new MessageHandler() {
            @Override
            public void handle(Message msg) throws Exception {
                if (msg instanceof TextMessage) {
                    TextMessage txt = (TextMessage) msg;
                    service.persistWithException(new Person(txt.getText(), 1));
                }
            }
        });
        messageProducer.generateMessages();
        Thread.sleep(1000);
        Assert.assertEquals(0, service.fetch().size());
        Assert.assertEquals(10, messageListener.getMessageCounter());
    }
}
