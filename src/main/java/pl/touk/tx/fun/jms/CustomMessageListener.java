package pl.touk.tx.fun.jms;

import org.apache.log4j.Logger;
import pl.touk.tx.fun.handler.MessageHandler;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

/**
 * @author mcl
 */
public class CustomMessageListener implements MessageListener {

    private Logger log = Logger.getLogger(CustomMessageListener.class);
    
    MessageHandler messageHandler;

    private int messageCounter = 0;

    @Override
    public void onMessage(Message message) {
        try {
            messageCounter++;
            log.info("Got: " + ((TextMessage)message).getText());
            if (messageHandler != null) {
                log.info("Processing message: " + ((TextMessage)message).getText());
                messageHandler.handle(message);
            }
        } catch (JMSException e) {
            log.error("Error getting <body> from JMS message: ", e);
        } catch (Exception e) {
            log.error("Unexpected error: ", e);
        }
    }

    public void setMessageHandler(MessageHandler messageHandler) {
        this.messageHandler = messageHandler;
    }

    public int getMessageCounter() {
        return messageCounter;
    }

    public void reset() {
        messageCounter = 0;
    }
}
