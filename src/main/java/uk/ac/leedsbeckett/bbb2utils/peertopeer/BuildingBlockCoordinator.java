/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.ac.leedsbeckett.bbb2utils.peertopeer;

import java.util.ArrayList;
import java.util.Random;
import java.util.logging.Level;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import org.apache.log4j.Logger;

/**
 *
 * @author jon
 */
public class BuildingBlockCoordinator implements PeerDestinationListener
{
  Logger logger;
  DestinationManager destinationmanager;
  ProducingPeerDestination destination;
  ArrayList<String> knownpeers = new ArrayList<>();
  String buildingblockvid, buildingblockhandle, serverid, pluginid;
  BuildingBlockPeerMessageListener listener;
  int pingrate=0;

  Object pingerlock = new Object(); 
  PingerThread pingerthread = null;
  
  public BuildingBlockCoordinator( String buildingblockvid, String buildingblockhandle, String serverid, BuildingBlockPeerMessageListener listener, Logger logger ) throws JMSException
  {
    this.logger = logger;
    this.buildingblockvid = buildingblockvid;
    this.buildingblockhandle = buildingblockhandle;
    this.serverid = serverid;
    this.listener = listener;
    pluginid = buildingblockvid + "_" + buildingblockhandle;
    destinationmanager = new DestinationManager( logger );
    destination = destinationmanager.createPeerDestination( buildingblockvid, buildingblockhandle, serverid, this );
    destinationmanager.start();
    sendStartingMessage();
    sendDiscoverMessage();
  }
  
  public void destroy() throws JMSException
  {
    sendStoppingMessage();
    destinationmanager.release();    
  }
  
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
  
  @Override
  public void consumeMessage(PeerDestination destination, Message message)
  {
    try
    {
      String to = message.getStringProperty( "LBUToServerID" );
      if ( to == null || ( !"*".equals(to) && !serverid.equals(to) ) )
        return;
      String type = message.getStringProperty( "LBUType" );
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
      logger.info( serverid + " received " + subtype + " from " + from );
      switch ( subtype )
      {
        case "STOPPING":
          if ( knownpeers.contains( from ) )
          {
            knownpeers.remove( from );
            updating=true;
          }
          break;
        case "DISCOVER":
          sendRunningMessage();
        case "STARTING":
        case "RUNNING":
          if ( !knownpeers.contains( from ) )
          {
            knownpeers.add( from );
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
        for ( String name : knownpeers )
          logger.info( name );
        logger.info( "-----------------------" );
      }
    }
  }
  
  public void sendTextMessageToAll( String str ) throws JMSException
  {
    sendTextMessage( str, "*" );
  }
 
  public synchronized void sendTextMessage( String str, String toserverid ) throws JMSException
  {
    TextMessage message = destination.createTextMessage();
    message.setStringProperty( "LBUToServerID",    toserverid     );
    message.setStringProperty( "LBUFromServerID",  serverid       );
    message.setStringProperty( "LBUPluginID",      pluginid       );
    message.setStringProperty( "LBUType",          "" );
    message.setStringProperty( "LBUSubType",       ""        );
    message.setText( str );
    destination.send( message );
  }
 
  void sendCoordinationMessage( String command ) throws JMSException
  {
    sendCoordinationMessage( command, "*" );
  }
  
  synchronized void sendCoordinationMessage( String command, String to ) throws JMSException
  {
    TextMessage message = destination.createTextMessage();
    message.setStringProperty( "LBUToServerID",    to             );
    message.setStringProperty( "LBUFromServerID",  serverid       );
    message.setStringProperty( "LBUPluginID",      pluginid       );
    message.setStringProperty( "LBUType",          "coordination" );
    message.setStringProperty( "LBUSubType",       command        );
    message.setText( "" );
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
          try { sendPingMessage(); } catch ( JMSException ex ) {}
        }
        catch (InterruptedException ex) {  }
      }
    }    
  }
}
