/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.ac.leedsbeckett.peertopeer;

import blackboard.platform.config.ConfigurationServiceFactory;
import java.util.ArrayList;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 *
 * @author jon
 */
public class DestinationManager implements AmqUriListener, ExceptionListener
{
  Logger logger;
  
  String currenturi = null;
  Connection connection;
  Session session;
  
  ArrayList<PeerDestinationImpl> destinations = new ArrayList<>();
  
  public DestinationManager( Logger logger )
  {
    if ( logger != null )
      this.logger = logger;
    else
    {
      this.logger = Logger.getLogger(this.getClass());
      this.logger.setLevel( Level.OFF );
    }
    AmqHelper helper = AmqHelperFactory.getAMQHelper();
    helper.registerListener( this );
  }

  @Override
  public synchronized void processAmqUriChange( String uri )
  {
    logger.info( "New uri for AMQ broker = " + uri );
    closeAll();
    currenturi = uri;
    openAll();
  }    
  
  public synchronized void openAll()
  {
    if ( currenturi == null )
      return;
    
    connection = null;
    session = null;
    
    try
    {  
      ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory( currenturi );
      connectionFactory.setUserName( "messageQueueService" );
      connectionFactory.setPassword( ConfigurationServiceFactory.getInstance().getBbProperty("bbconfig.messagequeue.password") );
      connection = connectionFactory.createConnection();
      connection.start();     
      connection.setExceptionListener( this );
      session = connection.createSession( false, Session.AUTO_ACKNOWLEDGE );
      logger.info( "Connected to AMQ at " + currenturi );
      for ( PeerDestinationImpl d : destinations )
      {
        try
        {  
          d.open();
          logger.info( "Opened destination " + d.name );
        }
        catch (JMSException ex)
        {
          logger.info( "Exception trying to open destination " + d.name , ex );
        }
      }
    }
    catch (JMSException ex)
    {
      logger.info( "Exception trying to connect to AMQ " , ex );
    }
  }
  
  private synchronized void closeAll()
  {
    for ( PeerDestinationImpl d : destinations )
    {
      try
      {
        d.close();
        logger.info( "Closed destination " + d.name );
      }
      catch (JMSException ex)
      {
        logger.info( "Exception trying to close destination " + d.name, ex );
      }
    }
      
    if ( session != null )
      try { session.close(); }
      catch ( Exception ex ) {logger.info( "Unable to close session.", ex );}
      finally { session = null; }
    if ( connection != null )
      try { connection.close(); }
      catch ( Exception ex ) {logger.info( "Unable to close connection.", ex );}
      finally { connection = null; }
  }

  
  public synchronized PeerDestination createPeerDestination( String name, PeerDestinationListener listener )
          throws JMSException
  {
    return createPeerDestination( name, false, false, listener );
  }
  
  public synchronized ProducingPeerDestination createProducingPeerDestination( String name )
          throws JMSException
  {
    return createPeerDestination( name, false, true, null );
  }

  public synchronized ProducingPeerDestination createProducingPeerDestination( String name, PeerDestinationListener listener )
          throws JMSException
  {
    return createPeerDestination( name, false, true, listener );
  }
  
  public synchronized PeerDestination createPeerTopicDestination( String name, PeerDestinationListener listener )
          throws JMSException
  {
    return createPeerDestination( name, true, false, listener );
  }
  
  public synchronized ProducingPeerDestination createProducingPeerTopicDestination( String name )
          throws JMSException
  {
    return createPeerDestination( name, true, true, null );
  }

  public synchronized ProducingPeerDestination createProducingPeerTopicDestination( String name, PeerDestinationListener listener )
          throws JMSException
  {
    return createPeerDestination( name, true, true, listener );
  }
  
  private synchronized PeerDestinationImpl createPeerDestination( String name, boolean istopic, boolean isproducer, PeerDestinationListener listener ) throws JMSException
  {
    logger.info( "Creating destination " + name );
    PeerDestinationImpl pd = new PeerDestinationImpl( name, istopic, isproducer, listener );
    pd.open();
    destinations.add( pd );
    return pd;
  }
  
  public synchronized void releasePeerDestination( PeerDestination pd ) throws JMSException
  {
    if ( pd instanceof PeerDestinationImpl )
    {
      PeerDestinationImpl pdi = (PeerDestinationImpl)pd;
      logger.info( "Releasing destination " + pdi.name );
      pdi.close();
      pdi.destroy();
    }
  }
  
  @Override
  public void onException(JMSException jmse)
  {
  }
  
  
  
  class PeerDestinationImpl implements ProducingPeerDestination, Runnable
  {
    final String  name;
    final boolean istopic;
    final boolean isproducer;
    final boolean isconsumer;
    final PeerDestinationListener listener;
    
    Destination     destination;
    MessageProducer producer;
    MessageConsumer consumer;
    Thread          consumerthread;

    boolean destroying = false;
    
    PeerDestinationImpl( String name, boolean istopic, boolean isproducer, PeerDestinationListener listener )
    {
      this.name       = name;
      this.istopic    = istopic;
      this.isproducer = isproducer;
      this.isconsumer = listener != null;
      this.listener   = listener;
      if ( isconsumer )
      {
        consumerthread = new Thread( this );
        consumerthread.start();
      }
    }

    void open() throws JMSException
    {
      if ( istopic )
        destination = session.createTopic( name );
      else
        destination = session.createQueue( name );
      if ( isproducer )
      {
        producer = session.createProducer( destination );
        producer.setDeliveryMode( DeliveryMode.NON_PERSISTENT );
      }
      if ( isconsumer )
      {
        consumer = session.createConsumer( destination );
        consumerthread.interrupt();
      }
    }

    void close() throws JMSException
    {
      if ( isconsumer )
      {
        consumer.close();
        consumer = null;
      }
    }

    void destroy()
    {
      destroying = true;
      consumerthread.interrupt();
    }

    @Override
    public void send( String str ) throws JMSException
    {
      if ( producer == null )
      {
        logger.info( name + " Can't send message, there is no producer." );
        return;
      }
      TextMessage message = session.createTextMessage();
      message.setText( str );
      producer.send( message );
    }
    
    @Override
    public void run()
    {
      while ( !destroying )
      {
        while ( consumer == null )
        {
          //logger.info( name + " Waiting for consumer to be created." );
          try { Thread.sleep( 60000 ); }
          catch (InterruptedException ex) {}
        }
        
        try
        {
          //logger.info( name + " Waiting for incoming message." );
          Message message = consumer.receive( 60000 );
          if ( message != null )
          {
            logger.info( name + " Message received" );
            listener.consumeMessage( this, message );
          }
        }
        catch (JMSException ex) {}
      }
      logger.info( name + " PeerDestinationImpl thread ending." );
    }
  }
  
}
