package pl.touk.tx.fun.jms;

import org.apache.log4j.Logger;
import org.springframework.jms.JmsException;
import pl.touk.tx.fun.handler.MessageHandler;

import javax.jms.*;

/**
 * @author mcl
 */
public class CustomMessageListener implements MessageListener {

    private Logger log = Logger.getLogger(CustomMessageListener.class);
    
    MessageHandler messageHandler;

    @Override
    public void onMessage(Message message) throws JmsException {
        try {
            log.info("Got: " + ((TextMessage)message).getText());
            if (messageHandler != null) {
                log.info("Processing message: " + ((TextMessage)message).getText());
                messageHandler.handle(message);
            }
        } catch (JmsException e) {
            log.error("Error getting <body> from JMS message: ", e);
            throw e; // was missing
        } catch (Exception e) {
            log.error("Unexpected error: ", e);
            throw new RuntimeException("Test listener threw exception", e);
        }
    }

    public void setMessageHandler(MessageHandler messageHandler) {
        this.messageHandler = messageHandler;
    }
}
