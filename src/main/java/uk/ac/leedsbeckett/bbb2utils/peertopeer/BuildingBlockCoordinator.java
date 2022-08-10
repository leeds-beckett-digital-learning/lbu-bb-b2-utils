/*
 * Copyright 2022 Leeds Beckett University.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.leedsbeckett.bbb2utils.peertopeer;

import blackboard.platform.BbServiceManager;
import blackboard.platform.messagequeue.MessageQueueException;
import blackboard.platform.messagequeue.MessageQueueService;
import blackboard.platform.messagequeue.MessageQueueUtil;
import blackboard.platform.messagequeue.impl.activemq.ActiveMQConnectionPool;
import blackboard.platform.messagequeue.impl.activemq.ActiveMQMessageQueueService;
import blackboard.platform.messagequeue.impl.activemq.ActiveMQTopicSubscriber;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.TopicPublisher;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQTopicSession;
import org.apache.log4j.Logger;

/**
 * This is used by a building block to coordinate between instances running on
 * a cluster of servers.  To use it the building block needs to have this
 * permission added to its manifest:
 * &lt;permission type="socket" name="*" actions="connect,resolve"/&gt;
 * @author jon
 */
public class BuildingBlockCoordinator extends ActiveMQTopicSubscriber
{
  public static final int PEER_TIMEOUT = 15 * 60 * 1000;
  
  boolean started = false;
  boolean failed = false;
  
  final Logger logger;
  final String serverid, pluginid, topic;
  
  
  HashMap<String,PeerRecord> knownpeers = new HashMap<>();
  ArrayList<PeerRecord> peersbyage = new ArrayList<>();
  long starttime;
  BuildingBlockPeerMessageListener listener;
  int pingrate=0;

  Object pingerlock = new Object(); 
  PingerThread pingerthread = null;

/**
 * Intantiates, but doesn't start, a tool to allow messaging between
 * instances of a building block. Parameters are used to identify the name
 * of the JMS destination that will be used.
 * 
 * @param buildingblockvid Identifies the destination.
 * @param buildingblockhandle Identifies the destination.
 * @param serverid Identifies this instance, distinguishing it from others.
 * @param listener Object that will be told about incoming messages.
 * @param logger A custom logger for errors.
 * @throws JMSException 
 */  
  public BuildingBlockCoordinator( String buildingblockvid, String buildingblockhandle, String serverid, BuildingBlockPeerMessageListener listener, Logger logger ) throws JMSException
  {
    super( "lbucoordination_" + buildingblockvid + "_" + buildingblockhandle );
    this.pluginid = buildingblockvid + "_" + buildingblockhandle;
    this.topic = "lbucoordination_" + pluginid;
    this.logger = logger;
    this.serverid = serverid;
    this.listener = listener;
  }
  
  /**
   * Starts the messaging asynchronously. It creates a thread that will
   * start messaging after a short delay but returns immediately.
   */
  public void start()
  {
    started = true;
    starttime = System.currentTimeMillis();
    register();
    StarterThread st = new StarterThread();
    st.start();
  }
  
  /**
   * Stops the messaging system such that it cannot be started again.
   * @throws JMSException 
   */
  public void destroy() throws JMSException
  {
    setPingRate( 0 );
    unregister();
    if ( started && !failed )
      sendStoppingMessage();
  }
  
  /**
   * Pings are periodically sent and peers will send pongs back. This is
   * for debugging. Default is '0'.
   * @param newpingrate In seconds - although a random element is used 
   * from 50% to 150% of the value set. Zero means switch off pings.
   */
  public void setPingRate( int newpingrate )
  {
    synchronized ( pingerlock )
    {
      if ( newpingrate <= 0 )
      {
        if ( pingerthread != null )
        {
          pingerthread.running = false;
          pingerthread.interrupt();
          pingerthread = null;
        }
        pingrate = 0;
        return;
      }

      pingrate = newpingrate;
      if ( pingerthread == null )
      {
        pingerthread = new PingerThread();
        pingerthread.start();
      }
      else
      {
        pingerthread.interrupt();
      }
    }
  }
  
  void consumeCoordinationMessage( Message message ) throws JMSException
  {
    if ( message instanceof TextMessage )
    {
      boolean updating = false;
      TextMessage tm = (TextMessage)message;
      String subtype = message.getStringProperty( "LBUSubType" );
      String from = message.getStringProperty( "LBUFromServerID" );
      logger.debug( "<----- " + serverid + " received " + subtype + " from " + from );
      switch ( subtype )
      {
        case "STOPPING":
          if ( this.hasPeer( from ) )
          {
            this.removePeer( from );
            updating=true;
          }
          break;
          
        case "DISCOVER":
          sendRunningMessage();
        case "STARTING":
        case "RUNNING":
          if ( !this.hasPeer( from ) )
          {
            long fromstarttime = Long.parseLong( message.getStringProperty( "LBUFromServerStartTime" ) );
            this.addPeer( from, fromstarttime );
          }
          touchPeer( from );
          updating=true;
          break;
          
        case "PING":
          if ( !serverid.equals( from ) )
            sendPongMessage( from );
        case "PONG":
          touchPeer( from );
          updating=true;
          break;
        default:
          break;
      }

      if ( updating )
      {
        thinPeers();
        logPeers();
      }
    }
  }
  
  /**
   * Send an arbitrary message that peers will understand. Send it to all the
   * connected peers.
   * 
   * @param str The message.
   * @throws JMSException 
   */
  public void sendTextMessageToAll( String str ) throws JMSException
  {
    sendTextMessage( str, "*" );
  }
 
  /**
   * Send an arbitrary message that peers will understand. Send it to the
   * oldest connected peer.
   * 
   * @param str The message.
   * @throws JMSException 
   */
  public void sendTextMessageToOldest( String str ) throws JMSException
  {
    String name = getOldestPeerName();
    if ( name != null)
      sendTextMessage( str, name );
  }
 
  /**
   * Send an arbitrary message that peers will understand. Send it to the
   * oldest connected peer.
   * 
   * @param str The message.
   * @throws JMSException 
   */
  public void sendTextMessageToNewest( String str ) throws JMSException
  {
    String name = getNewestPeerName();
    if ( name != null)
      sendTextMessage( str, name );
  }
 
  
  
  public void sendMessage( String text, Properties properties ) throws MessageQueueException
  {
    ActiveMQConnection con = null;
    ActiveMQTopicSession session = null;
    TopicPublisher publisher = null;
    final ActiveMQConnectionPool activeMQConnectionPool = this.getConnectionPool();
    try
    {
      con = (ActiveMQConnection) activeMQConnectionPool.get();
      session = (ActiveMQTopicSession) con.createTopicSession(false, 1);
      publisher = session.createPublisher( session.createTopic( topic ) );
      TextMessage message = session.createTextMessage( text );
      for ( Object key : properties.keySet() )
        message.setStringProperty( key.toString(), properties.getProperty( key.toString() ) );
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
  
  
  
  /** 
   * Send an arbitrary message that peers will understand.
   * @param str The message
   * @param toserverid The server that should receive the message.
   * @throws JMSException 
   */
  public synchronized void sendTextMessage( String str, String toserverid ) throws JMSException
  {
    if ( !started || failed )
    {
      logger.error( "Unable to send text message. Not started or starting failed." );
      return;
    }
    Properties p = new Properties();
    p.setProperty( "LBUToServerID",    toserverid     );
    p.setProperty( "LBUFromServerID",  serverid       );
    p.setProperty( "LBUPluginID",      pluginid       );
    p.setProperty( "LBUType",          "" );
    p.setProperty( "LBUSubType",       ""        );

    logger.debug( "-----> " + serverid + " sending user message to " + toserverid );
    try { sendMessage( str, p ); } catch (MessageQueueException ex) { logger.error( "Unable to send message via BB connection pool. ", ex );
    }
  }
 
  void sendCoordinationMessage( String command ) throws JMSException
  {
    sendCoordinationMessage( command, "*" );
  }
  
  synchronized void sendCoordinationMessage( String command, String to ) throws JMSException
  {
    if ( !started || failed )
    {
      logger.error( "Unable to send text message. Not started or starting failed." );
      return;
    }
    Properties p = new Properties();
    p.setProperty( "LBUToServerID",          to                         );
    p.setProperty( "LBUFromServerID",        serverid                   );
    p.setProperty( "LBUFromServerStartTime", Long.toString( starttime ) );
    p.setProperty( "LBUPluginID",            pluginid                   );
    p.setProperty( "LBUType",                "coordination"             );
    p.setProperty( "LBUSubType",             command                    );
    
    logger.debug( "-----> " + serverid + " sending coordination " + command + " to " + to );
    try { sendMessage( "", p ); } catch (MessageQueueException ex) { logger.error( "Unable to send message via BB connection pool. ", ex ); }
  }
  
  void sendStartingMessage() throws JMSException
  {
    sendCoordinationMessage( "STARTING" );
  }

  void sendDiscoverMessage() throws JMSException
  {
    sendCoordinationMessage( "DISCOVER" );
  }

  void sendRunningMessage() throws JMSException
  {
    sendCoordinationMessage( "RUNNING" );
  }

  void sendStoppingMessage() throws JMSException
  {
    sendCoordinationMessage( "STOPPING" );
  }

  void sendPingMessage() throws JMSException
  {
    sendCoordinationMessage( "PING" );
  }
  
  void sendPongMessage( String to ) throws JMSException
  {
    sendCoordinationMessage( "PONG", to );
  }

  @Override
  public void onMessage( Message message )
  {
    try
    {
      String to = message.getStringProperty( "LBUToServerID" );
      if ( to == null || ( !"*".equals(to) && !serverid.equals(to) ) )
        return;
      String type = message.getStringProperty( "LBUType" );
      if ( "coordination".equals( type ) )
        consumeCoordinationMessage( message );
      else if ( listener != null )
      {
        logger.debug( "<----- " + serverid + " received user message from " + message.getStringProperty( "LBUFromServerID" ) );
        listener.consumeMessage( message );
      }
    }
    catch (JMSException ex)
    {
      logger.error( "Exception while processing incoming message.", ex );
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

  
  class StarterThread extends Thread
  {
    @Override
    public void run()
    {
      try { Thread.sleep( 1000 ); }
      catch (InterruptedException ex) {}
      try
      {        
        sendStartingMessage();
        sendDiscoverMessage();
      }
      catch (JMSException ex)
      {
        logger.error( "Exception trying to start messaging system.", ex );
        failed = true;
      }
    }
  }

  class PingerThread extends Thread
  {
    boolean running = false;
    Random random = new Random( System.currentTimeMillis() );

    public PingerThread()
    {
      super( "BuildingBlockCoordinator.PingerThread" );
    }
    
    @Override
    public synchronized void start()
    {
      running = true;
      super.start();
    }

    @Override
    public void run()
    {
      while ( running )
      {
        try
        { 
          Thread.sleep( (pingrate/2 + random.nextInt(pingrate) )*1000L );
          if ( started && !failed )
            try { sendPingMessage(); } catch ( JMSException ex ) {}
        }
        catch (InterruptedException ex) {  }
      }
    }    
  }

  synchronized List<PeerRecord> getPeerRecordList()
  {
    LinkedList<PeerRecord> list = new LinkedList<>();
    for ( PeerRecord r : this.peersbyage )
      list.add( r );
    return list;
  }
  
  synchronized String getOldestPeerName()
  {
    thinPeers();
    if ( this.peersbyage.isEmpty() ) return null;
    return this.peersbyage.get( 0 ).name;
  }
  
  synchronized String getNewestPeerName()
  {
    thinPeers();
    if ( this.peersbyage.isEmpty() ) return null;
    return this.peersbyage.get( this.peersbyage.size() - 1 ).name;
  }
  
  synchronized PeerRecord getPeer( String name )
  {
    return this.knownpeers.get( name );
  }
  
  synchronized boolean hasPeer( String name )
  {
    return this.knownpeers.containsKey( name );
  }
  
  synchronized void removePeer( String name )
  {
    PeerRecord record = this.knownpeers.get( name );
    this.knownpeers.remove( name );
    this.peersbyage.remove( record );
  }
  
  synchronized void addPeer( String name, long starttime )
  {
    PeerRecord record = new PeerRecord( name, starttime );
    this.knownpeers.put( name, record );
    this.peersbyage.add( record );
    this.peersbyage.sort(( PeerRecord o1, PeerRecord o2 ) ->
    {
      if ( o1.starttime < o2.starttime ) return -1;
      if ( o1.starttime > o2.starttime ) return  1;
      return 0;
    });
  }
  
  synchronized void touchPeer( String name )
  {
    if ( !hasPeer( name ) )
      return;
    knownpeers.get( name ).lastcontacttime = System.currentTimeMillis();
  }
  
  synchronized void thinPeers()
  {
    long now = System.currentTimeMillis();
    boolean updated = false;
    for ( PeerRecord record : getPeerRecordList() )
    {
      long age = now - record.lastcontacttime;
      if ( age > PEER_TIMEOUT )
      {
        removePeer( record.name );
        updated = true;
      }
    }
    if ( updated )
      logPeers();
  }
  
  void logPeers()
  {
    logger.info( "-----------------------" );
    logger.info( "Peer list." );
    long now = System.currentTimeMillis();
    for ( PeerRecord r : this.getPeerRecordList() )
      logger.info( r.name + " Started " + ((now - r.starttime)/1000L) + "s ago. Last heard from " + (now - r.lastcontacttime) + "ms ago." );
    logger.info( "-----------------------" );    
  }
  
  class PeerRecord
  {
    String name;
    long starttime;
    long lastcontacttime;

    public PeerRecord( String name, long starttime )
    {
      this.name = name;
      lastcontacttime = this.starttime = starttime;
    }
    
  }
}
