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

    @Autowired ApplicationContext ctx;
    
    @Autowired SimpleDao dao;

    @Autowired SimplePersonService service;
    @Autowired CustomMessageListener messageListener;
    @Autowired SampleMessageProducer messageProducer;
    @Autowired DefaultMessageListenerContainer container;

    Broker broker;
    
    @Before
    public void setup() throws Exception {
        dao.update("drop table if exists persons");
        dao.update("create table persons (name varchar, age integer)");
        broker = ((XBeanBrokerService) ctx.getBean("my-broker")).getBroker();
        getStats("sample.messages").reset();
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
        Assert.assertEquals(10, getStats("sample.messages").getDequeues().getCount());
        Assert.assertEquals(0, getStats("sample.messages").getMessages().getCount());
    }

    /**
     * We consume messages from the queue event thou there has been an exception.
     * 
     * Messages are removed from the broker and can be lost due to errors on listener's side.
     *
     * Messages counter should be > 0
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
        Thread.sleep(2000);
        Assert.assertEquals(0, service.fetch().size());
        Assert.assertEquals(10, getStats("sample.messages").getDequeues().getCount());
        Assert.assertEquals(0, getStats("sample.messages").getMessages().getCount());
    }

    /**
     * Even thou we call <i>persistWithException</i> - which has @Transactional annotation, it does not
     * stop the listener from removing message from JMS queue.
     *
     * Messages counter should be > 0
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
        Assert.assertEquals(10, getStats("sample.messages").getDequeues().getCount());
        Assert.assertEquals(0, getStats("sample.messages").getMessages().getCount());
    }
    
    private DestinationStatistics getStats(String name) {
        for (ActiveMQDestination destination : broker.getDestinationMap().keySet()) {
            if (destination.getQualifiedName().equals("queue://" + name))
                return broker.getDestinationMap().get(destination).getDestinationStatistics();
        }
        return null;
    }
}
