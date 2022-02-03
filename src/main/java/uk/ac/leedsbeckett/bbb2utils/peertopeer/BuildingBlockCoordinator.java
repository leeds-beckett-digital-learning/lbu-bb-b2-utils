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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import org.apache.log4j.Logger;

/**
 * This is used by a building block to coordinate between instances running on
 * a cluster of servers.  To use it the building block needs to have this
 * permission added to its manifest:
 * &lt;permission type="socket" name="*" actions="connect,resolve"/&gt;
 * @author jon
 */
public class BuildingBlockCoordinator implements PeerDestinationListener
{
  boolean started = false;
  boolean failed = false;
  
  Logger logger;
  DestinationManager destinationmanager;
  ProducingPeerDestination destination;
  HashMap<String,PeerRecord> knownpeers = new HashMap<>();
  ArrayList<PeerRecord> peersbyage = new ArrayList<>();
  String buildingblockvid, buildingblockhandle, serverid, pluginid;
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
    this.logger = logger;
    this.buildingblockvid = buildingblockvid;
    this.buildingblockhandle = buildingblockhandle;
    this.serverid = serverid;
    this.listener = listener;
    pluginid = buildingblockvid + "_" + buildingblockhandle;
  }
  
  /**
   * Starts the messaging asynchronously. It creates a thread that will
   * start messaging after a short delay but returns immediately.
   */
  public void start()
  {
    StarterThread st = new StarterThread();
    st.start();
  }
  
  /**
   * Stops the messaging system such that it cannot be started again.
   * @throws JMSException 
   */
  public void destroy() throws JMSException
  {
    if ( started && !failed )
      sendStoppingMessage();
    destinationmanager.release();    
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
  
  /**
   * Don't use this method - it is for internal use.
   * @param destination
   * @param message 
   */
  @Override
  public void consumeMessage(PeerDestination destination, Message message)
  {
    logger.debug( "Consuming message." );
    try
    {
      String to = message.getStringProperty( "LBUToServerID" );
      if ( to == null || ( !"*".equals(to) && !serverid.equals(to) ) )
        return;
      String type = message.getStringProperty( "LBUType" );
      logger.debug( "Consuming message of type " + type );
      if ( "coordination".equals( type ) )
        consumeCoordinationMessage( destination, message );
      else if ( listener != null )
        listener.consumeMessage( message );
    }
    catch (JMSException ex)
    {
      logger.error( "Exception while consuming message ", ex );
    }
  }
  
  void consumeCoordinationMessage(PeerDestination destination, Message message) throws JMSException
  {
    if ( message instanceof TextMessage )
    {
      boolean updating = false;
      TextMessage tm = (TextMessage)message;
      String subtype = message.getStringProperty( "LBUSubType" );
      String from = message.getStringProperty( "LBUFromServerID" );
      logger.debug( serverid + " received " + subtype + " from " + from );
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
            updating=true;
          }
          break;
        case "PING":
          if ( !serverid.equals( from ) )
            sendPongMessage( from );
          break;
        case "PONG":
          break;
        default:
          break;
      }

      if ( updating )
      {
        logger.info( "-----------------------" );
        logger.info( "Updated peer list." );
        for ( PeerRecord r : this.getPeerRecordList() )
          logger.info( r.name + " started " + r.starttime );
        logger.info( "-----------------------" );
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
    TextMessage message = destination.createTextMessage();
    message.setStringProperty( "LBUToServerID",    toserverid     );
    message.setStringProperty( "LBUFromServerID",  serverid       );
    message.setStringProperty( "LBUPluginID",      pluginid       );
    message.setStringProperty( "LBUType",          "" );
    message.setStringProperty( "LBUSubType",       ""        );
    message.setText( str );
    logger.debug( "Sending text message." );
    destination.send( message );
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
    TextMessage message = destination.createTextMessage();
    message.setStringProperty( "LBUToServerID",          to                         );
    message.setStringProperty( "LBUFromServerID",        serverid                   );
    message.setStringProperty( "LBUFromServerStartTime", Long.toString( starttime ) );
    message.setStringProperty( "LBUPluginID",            pluginid                   );
    message.setStringProperty( "LBUType",                "coordination"             );
    message.setStringProperty( "LBUSubType",             command                    );
    message.setText( "" );
    logger.debug( "Sending coordination message." );
    destination.send( message );
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

  
  class StarterThread extends Thread
  {
    @Override
    public void run()
    {
      try { Thread.sleep( 1000 ); }
      catch (InterruptedException ex) {}
      logger.debug( "Starting building block coordinator." );
      
      try
      {
        destinationmanager = new DestinationManager( logger );
        destination = destinationmanager.createPeerDestination( 
                pluginid, 
                serverid, 
                BuildingBlockCoordinator.this );
        destinationmanager.start();    
        started = true;
        starttime = System.currentTimeMillis();
        logger.debug( "Destination manager started." );
        
        sendStartingMessage();
        logger.debug( "Sent 'starting'." );
        sendDiscoverMessage();
        logger.debug( "Sent 'discover'." );
        logger.debug( "Building block coordinator started." );
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
    if ( this.peersbyage.isEmpty() ) return null;
    return this.peersbyage.get( 0 ).name;
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
  
  class PeerRecord
  {
    String name;
    long starttime;

    public PeerRecord( String name, long starttime )
    {
      this.name = name;
      this.starttime = starttime;
    }
    
  }
}
