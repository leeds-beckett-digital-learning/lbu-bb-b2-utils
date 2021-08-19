/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.ac.leedsbeckett.peertopeer;

import javax.jms.Message;

/**
 *
 * @author jon
 */
public interface PeerDestinationListener
{
  public void consumeMessage( PeerDestination destination, Message message );
}
