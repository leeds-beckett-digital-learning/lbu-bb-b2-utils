/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.ac.leedsbeckett.bbb2utils.peertopeer;

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
  
  public DestinationManager(Logger logger)
  {
    if ( logger != null )
      this.logger = logger;
    else
    {
      logger = Logger.getLogger(this.getClass());
      logger.setLevel( Level.OFF );
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
    currenturi = "failover:(" + uri + ")?initialReconnectDelay=100&startupMaxReconnectAttempts=10&maxReconnectAttempts=20";
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
    {
      connection.setExceptionListener(null);
      connection.stop();
    }
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
          logger.info( "Opened destination " + d.pluginid );
        }
        catch (JMSException ex)
        {
          logger.info( "Exception trying to open destination " + d.pluginid , ex );
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
        logger.info( "Closed destination " + d.pluginid );
      }
      catch (JMSException ex)
      {
        logger.info( "Exception trying to close destination " + d.pluginid, ex );
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

  
  public synchronized ProducingPeerDestination createPeerDestination( String pluginid, String serverid, PeerDestinationListener listener )
          throws JMSException
  {
    return DestinationManager.this.createPeerDestination( pluginid, serverid, true, true, listener );
  }
  
  private synchronized PeerDestinationImpl createPeerDestination( String pluginid, String serverid, boolean istopic, boolean isproducer, PeerDestinationListener listener ) throws JMSException
  {
    if ( started )
      throw new JMSException( "Cannot add destinations to DestinationManager when it has started." );
    logger.info( "Creating destination " + pluginid + ", " + serverid );
    PeerDestinationImpl pd = new PeerDestinationImpl( pluginid, serverid, istopic, isproducer, listener );
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
    final String  pluginid;
    final String  serverid;
    final boolean istopic;
    final boolean isproducer;
    final boolean isconsumer;
    final PeerDestinationListener listener;
    final String messageselector;
    
    Destination     destination;
    MessageProducer producer;
    MessageConsumer consumer;
    
    PeerDestinationImpl( String pluginid, String serverid, boolean istopic, boolean isproducer, PeerDestinationListener listener )
    {
      this.pluginid   = pluginid;
      this.serverid   = serverid;
      this.istopic    = istopic;
      this.isproducer = isproducer;
      this.isconsumer = listener != null;
      this.listener   = listener;
      
      messageselector = 
              "LBUPluginID = '" + pluginid + 
              "' AND ( LBUToServerID = '*' OR LBUToServerID = '" + serverid + "' )";
      
      logger.debug( "Message selector = {"  + messageselector + "}" );
    }

    void open() throws JMSException
    {
      if ( istopic )
        destination = session.createTopic(pluginid );
      else
        destination = session.createQueue(pluginid );
      if ( isproducer )
      {
        producer = session.createProducer( destination );
        producer.setDeliveryMode( DeliveryMode.NON_PERSISTENT );
        logger.info( "Opened producer" );
      }
      if ( isconsumer )
      {
        consumer = session.createConsumer( destination, messageselector );
        consumer.setMessageListener( this );
        logger.info( "Opened consumer." );
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
    public TextMessage createTextMessage() throws JMSException
    {
      return session.createTextMessage();
    }
    
    @Override
    public void send( Message message ) throws JMSException
    {
      if ( producer == null )
      {
        logger.error( pluginid + " Can't send message, there is no producer." );
        return;
      }
      logger.debug( pluginid + " Sending message." );
      producer.send( message );
    }
    
    @Override
    public void onMessage( Message message )
    {
      logger.debug( pluginid + " onMessage() called." );
      listener.consumeMessage( this, message );
    }

  }
  
}
