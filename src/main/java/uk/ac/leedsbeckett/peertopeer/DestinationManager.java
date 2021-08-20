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
import javax.jms.MessageListener;
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
  boolean started = false;
  
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

  /**
   * The AMQ helper tells this object if the message broker
   * goes down or comes up on a different uri.
   * @param uri 
   */
  @Override
  public synchronized void processAmqUriChange( String uri )
  {
    logger.info( "New uri for AMQ broker = " + uri );
    closeAll();
    currenturi = uri;
    openAll(); 
    if ( started )
      try { connection.start(); } catch (JMSException ex)
          { logger.error("Cannot start connection to JMS", ex); }
  }    

  /**
   * Start the JMS connection. If JMS connection is lost and reconnected
   * it will automatically start again.
   * @throws JMSException 
   */
  public void start() throws JMSException
  {
    started = true;
    if ( connection != null )
      connection.start();
  }
  
  /**
   * Stop the JMS connection. If JMS connection is lost and reconnected
   * it will NOT automatically start again.
   * @throws JMSException 
   */
  public void stop() throws JMSException
  {
    if ( connection != null )
      connection.stop();
    started = false;
  }
  
  /**
   * Irreversibly shut down this destination manager.
   * @throws JMSException 
   */
  public void release() throws JMSException
  {
    AmqHelper helper = AmqHelperFactory.getAMQHelper();
    helper.unregisterListener( this );
    stop();
    closeAll();
  }
  
  synchronized void openAll()
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
      connection.setExceptionListener( this );
      session = connection.createSession( false, Session.AUTO_ACKNOWLEDGE );
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
      logger.info( "Connected to AMQ at " + currenturi );
    }
    catch (JMSException ex)
    {
      logger.info( "Exception trying to connect to AMQ " , ex );
    }
  }
  
  synchronized void closeAll()
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
    if ( started )
      throw new JMSException( "Cannot add destinations to DestinationManager when it has started." );
    logger.info( "Creating destination " + name );
    PeerDestinationImpl pd = new PeerDestinationImpl( name, istopic, isproducer, listener );
    pd.open();
    destinations.add( pd );
    return pd;
  }
  
  @Override
  public void onException(JMSException jmse)
  {
    logger.error( "JMS reported exception.", jmse );
  }
  
  
  
  class PeerDestinationImpl implements ProducingPeerDestination, MessageListener
  {
    final String  name;
    final boolean istopic;
    final boolean isproducer;
    final boolean isconsumer;
    final PeerDestinationListener listener;
    
    Destination     destination;
    MessageProducer producer;
    MessageConsumer consumer;
    
    PeerDestinationImpl( String name, boolean istopic, boolean isproducer, PeerDestinationListener listener )
    {
      this.name       = name;
      this.istopic    = istopic;
      this.isproducer = isproducer;
      this.isconsumer = listener != null;
      this.listener   = listener;
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
        consumer.setMessageListener( this );
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
    public void onMessage( Message message )
    {
      listener.consumeMessage( this, message );
    }
  }
  
}
