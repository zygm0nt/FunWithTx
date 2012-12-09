package pl.touk.tx.fun;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.xbean.XBeanBrokerService;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import pl.touk.tx.fun.handler.MessageHandler;
import pl.touk.tx.fun.jms.CustomMessageListener;
import pl.touk.tx.fun.jms.SampleMessageProducer;
import pl.touk.tx.fun.model.Person;

import javax.jms.Message;
import javax.jms.TextMessage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author mcl
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/applicationCtx-withAMQ.xml"})
public class JmsPersonServiceTest {

    private Logger log = Logger.getLogger(JmsPersonServiceTest.class);

    @Autowired ApplicationContext ctx;

    @Autowired SimpleDao dao;

    @Autowired SimplePersonService service;
    @Autowired CustomMessageListener messageListener;
    @Autowired SampleMessageProducer messageProducer;
    @Autowired DefaultMessageListenerContainer container;

    Broker broker;
    public static final int MESSAGES_SENT = 10;

    @Before
    public void setup() throws Exception {
        dao.update("drop table if exists persons");
        dao.update("create table persons (name varchar, age integer)");
        broker = ((XBeanBrokerService) ctx.getBean("my-broker")).getBroker();
    }

    @Test
    @DirtiesContext
    public void shouldAddPersonFromJMS() throws Exception {
        final AtomicInteger messagesHandled = new AtomicInteger(0);
        messageListener.setMessageHandler(new MessageHandler() {
            @Override
            public void handle(Message msg) throws Exception {
                messagesHandled.incrementAndGet();
                if (msg instanceof TextMessage) {
                    TextMessage txt = (TextMessage) msg;
                    service.persist(new Person(txt.getText(), 1));
                }
            }
        });
        messageProducer.generateMessages();
        Thread.sleep(1000);
        Assert.assertEquals(10, messagesHandled.get());
        Assert.assertEquals(10, service.fetch().size());
        Assert.assertEquals(10, getStats("sample.messages").getDequeues().getCount());
        Assert.assertEquals(0, getStats("sample.messages").getMessages().getCount());
    }

    @Test
    @DirtiesContext
    public void shouldHandleMessageFromJMSWithException() throws Exception {
        final CountDownLatch latch = new CountDownLatch(MESSAGES_SENT * 2);
        messageListener.setMessageHandler(new MessageHandler() {
            @Override
            public void handle(Message msg) throws Exception {
                try {
                    throw new RuntimeException("Message processing exception");
                } finally {
                    latch.countDown();
                }
            }
        });
        messageProducer.generateMessages(MESSAGES_SENT);

        latch.await(); // wait for all the messages to be processed
        Thread.sleep(1000); // sleep another bit just to let the transactions finish off

        Assert.assertEquals(0, service.fetch().size());
        Assert.assertEquals(MESSAGES_SENT, getStats("sample.messages").getDequeues().getCount());

        DestinationStatistics dlqStats = getStats("ActiveMQ.DLQ");
        Assert.assertNotNull(dlqStats);
        Assert.assertEquals(MESSAGES_SENT, dlqStats.getEnqueues().getCount());
    }

    @Test
    @DirtiesContext
    public void shouldHandleMessagesWithTransactionalMethod() throws Exception {
        // the default behaviour of DefaultMessageListenerContainer is to do a single local redelivery
        // if you want more, you have to specify that at the broker in a destination policy
        // see https://activemq.apache.org/message-redelivery-and-dlq-handling.html
        final CountDownLatch latch = new CountDownLatch(MESSAGES_SENT * 2);
        messageListener.setMessageHandler(new MessageHandler() {
            @Override
            public void handle(Message msg) throws Exception {
                if (msg instanceof TextMessage) {
                    TextMessage txt = (TextMessage) msg;
                    try {
                        service.persistWithException(new Person(txt.getText(), 1));
                    } finally {
                        latch.countDown();
                    }
                }
            }
        });
        messageProducer.generateMessages(MESSAGES_SENT);

        latch.await(); // wait for all the messages to be processed
        Thread.sleep(1000); // sleep another bit just to let the transactions finish off

        // no redelivery policy is set in activemq, so messages go to a dead letter queue (default behaviour)
        DestinationStatistics dlqStats = getStats("ActiveMQ.DLQ");
        Assert.assertNotNull(dlqStats);
        Assert.assertEquals(MESSAGES_SENT, dlqStats.getEnqueues().getCount());

        Assert.assertEquals(0, service.fetch().size());
        Assert.assertEquals(MESSAGES_SENT, getStats("sample.messages").getDequeues().getCount());
    }

    private DestinationStatistics getStats(String name) {
        for (ActiveMQDestination destination : broker.getDestinationMap().keySet()) {
            if (destination.getQualifiedName().equals("queue://" + name))
                return broker.getDestinationMap().get(destination).getDestinationStatistics();
        }
        return null;
    }
}
