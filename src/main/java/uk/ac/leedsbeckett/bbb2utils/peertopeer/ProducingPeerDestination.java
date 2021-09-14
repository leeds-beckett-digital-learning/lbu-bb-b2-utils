/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.ac.leedsbeckett.bbb2utils.peertopeer;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

/**
 *
 * @author jon
 */
public interface ProducingPeerDestination extends PeerDestination
{
  public TextMessage createTextMessage() throws JMSException;
  public void send( Message message ) throws JMSException;
}
