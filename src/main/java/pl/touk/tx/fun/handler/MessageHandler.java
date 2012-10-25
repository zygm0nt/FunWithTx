package pl.touk.tx.fun.handler;

import javax.jms.Message;

/**
 * @author mcl
 */
public interface MessageHandler {
    void handle(Message msg) throws Exception;
}
