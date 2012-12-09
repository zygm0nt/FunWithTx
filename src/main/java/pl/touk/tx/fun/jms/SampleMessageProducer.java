package pl.touk.tx.fun.jms;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * @author mcl
 */
public class SampleMessageProducer {

    private static final Logger logger = Logger.getLogger(SampleMessageProducer.class);

    protected static final String MESSAGE_COUNT = "messageCount";

    @Autowired
    private JmsTemplate template;
    private int messageCount = 10;

    /**
     * Generates JMS messages
     */
    public void generateMessages() throws JMSException {
        generateMessages(10);
    }

    public void generateMessages(int messageCount) {
        for (int i = 0; i < messageCount; i++) {
            final int index = i;
            final String text = "Message number is " + i + ".";

            template.send(new MessageCreator() {
                public Message createMessage(Session session) throws JMSException {
                    TextMessage message = session.createTextMessage(text);
                    message.setIntProperty(MESSAGE_COUNT, index);

                    logger.info("Sending message: " + text);

                    return message;
                }
            });
        }

    }
}