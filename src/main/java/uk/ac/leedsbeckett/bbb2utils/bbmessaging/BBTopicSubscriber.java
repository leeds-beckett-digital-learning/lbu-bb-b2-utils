/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package uk.ac.leedsbeckett.bbb2utils.bbmessaging;

import blackboard.platform.BbServiceManager;
import blackboard.platform.messagequeue.MessageQueueException;
import blackboard.platform.messagequeue.MessageQueueService;
import blackboard.platform.messagequeue.MessageQueueUtil;
import blackboard.platform.messagequeue.impl.activemq.ActiveMQConnectionPool;
import blackboard.platform.messagequeue.impl.activemq.ActiveMQMessageQueueService;
import blackboard.platform.messagequeue.impl.activemq.ActiveMQTopicSubscriber;
import java.util.ArrayList;
import javax.jms.Message;
import javax.jms.TopicPublisher;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQTopicSession;

/**
 *
 * @author jon
 */
public class BBTopicSubscriber  extends ActiveMQTopicSubscriber
{
  private final ArrayList<BBMessageListener> listeners = new ArrayList<>();
  private final String topicName;
  
  public BBTopicSubscriber( String topicName )
  {
    super( topicName );
    this.topicName = topicName;
  }
  
  public void registerPeerDestinationListener( BBMessageListener listener )
  {
    synchronized ( listeners )
    {
      listeners.add( listener );
    }
  }
  
  public void unregisterPeerDestinationListener( BBMessageListener listener )
  {
    synchronized ( listeners )
    {
      listeners.remove( listener );
    }
  }
  
  
  public void sendMessage( final Message message ) throws MessageQueueException
  {
    ActiveMQConnection con = null;
    ActiveMQTopicSession session = null;
    TopicPublisher publisher = null;
    final ActiveMQConnectionPool activeMQConnectionPool = this.getConnectionPool();
    try
    {
      con = (ActiveMQConnection) activeMQConnectionPool.get();
      session = (ActiveMQTopicSession) con.createTopicSession(false, 1);
      publisher = session.createPublisher(session.createTopic( topicName ));
      publisher.publish( message );
    }
    catch ( Exception ex )
    {
      throw new MessageQueueException( "Could not send AMQ message.", ex);
    }
    finally
    {
      MessageQueueUtil.closeMessageQueueObjects( session, null, null, publisher, null);
      MessageQueueUtil.releaseConnection(activeMQConnectionPool, con);
    }
  }

  ActiveMQConnectionPool getConnectionPool() {
    if (BbServiceManager.isServiceInitialized(MessageQueueService.class.getName())) {
      ActiveMQMessageQueueService activeMQMessageQueueService
              = (ActiveMQMessageQueueService) BbServiceManager.safeLookupService(MessageQueueService.class);
      return activeMQMessageQueueService.getConnectionPool();
    }
    return null;
  }

  @Override
  public void onMessage(Message msg)
  {
    BBMessageListener[] list;
    synchronized ( listeners )
    {
      list = listeners.toArray( new BBMessageListener[(listeners.size())] );
    }
    for ( BBMessageListener l : list )
      l.onMessage( msg );
  }

}
