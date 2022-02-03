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
package uk.ac.leedsbeckett.bbb2utils.coordination;

import com.xythos.common.api.XythosException;
import com.xythos.fileSystem.events.EventSubQueue;
import com.xythos.fileSystem.events.FileSystemEntryPropertyAddedEventImpl;
import com.xythos.fileSystem.events.StorageServerEventBrokerImpl;
import com.xythos.fileSystem.events.StorageServerEventListener;
import com.xythos.security.api.Context;
import com.xythos.security.api.ContextFactory;
import com.xythos.storageServer.api.FileSystem;
import com.xythos.storageServer.api.FileSystemEntry;
import com.xythos.storageServer.api.FileSystemEntryPropertyAddedEvent;
import com.xythos.storageServer.api.FileSystemEvent;
import com.xythos.storageServer.api.FileSystemFile;
import com.xythos.storageServer.api.VetoEventException;
import com.xythos.storageServer.properties.api.Property;
import com.xythos.storageServer.properties.api.PropertyDefinition;
import java.io.ByteArrayOutputStream;
import java.util.Properties;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import uk.ac.leedsbeckett.bbb2utils.XythosUtils;
import uk.ac.leedsbeckett.bbb2utils.messaging.MessageHeader;
import uk.ac.leedsbeckett.bbb2utils.messaging.MessageUtils;
import uk.ac.leedsbeckett.bbb2utils.json.JsonConvertor;

/**
 *
 * @author jon
 */
public abstract class InterserverCoordinationClient implements StorageServerEventListener
{
  static final Class[] listensfor = 
  {
    FileSystemEntryPropertyAddedEventImpl.class,
  };
  
  Logger logger = LogManager.getLogger(InterserverCoordinationClient.class );
  
  JsonConvertor<MessageHeader> mhjsonconv   = new JsonConvertor<>(MessageHeader.class);
  
  boolean monitoringxythos=false;
  XythosUtils xythosutils;
  String username;
  String pluginid;
  String serverid;
  
  boolean errorstate=false;
  
  String exchangepath = "/internal/plugins/exchange/";
  
  String rxlocalpropname;
  PropertyDefinition propdefrxlocally=null;
  PropertyDefinition propdefdestination=null;
  
  public InterserverCoordinationClient()
  {
  }

  public String getPluginId()
  {
    return pluginid;
  }

  public String getServerId()
  {
    return serverid;
  }

  public Logger getLogger()
  {
    return logger;
  }

  public String getExchangePath()
  {
    return exchangepath;
  }
  
  
  
  public InterserverCoordinationClient( Logger logger )
  {
    this.logger = logger;
  }

  public boolean isFailed()
  {
    return errorstate;
  }

  public XythosUtils getXythosUtils()
  {
    return xythosutils;
  }
  
  public void initialize( String username, String pluginid, String serverid )
  {
    this.username = username;
    this.pluginid = pluginid;
    this.serverid = serverid;
    Context context = null;
    try
    {
      xythosutils = new XythosUtils( username );
      xythosutils.setLogger( logger );
      rxlocalpropname = XythosUtils.COORDINATION_PROPERTY_RXLOCALLY + "_" + serverid;
      context = ContextFactory.create( xythosutils.getXythosUser(), new Properties() );
      if ( !xythosutils.createDirectories( context, exchangepath ) )
        errorstate=true;
      propdefrxlocally = xythosutils.getOrCreatePropertyDefinition( 
              XythosUtils.COORDINATION_PROPERTY_RXLOCALLY + "_" + serverid, 
              "Indicates to processes in the local server that message has been received.", 
              context );
      propdefdestination = xythosutils.getOrCreatePropertyDefinition( 
              XythosUtils.COORDINATION_PROPERTY_DESTINATION, 
              "Indicates to local server that message has been sent.", 
              context );
    }
    catch (Exception ex)
    {
      logger.error( "Exception trying to initialise Xythos content collection files.", ex );
      if ( context != null )
        try { context.rollbackContext(); }
        catch (XythosException ex1) { logger.error( "Unable to roll back Xythos context after exception", ex1 ); }
      errorstate=true;
    }
    finally
    {
      // commit regardless of whether it was necessary to create the file.
      // this is to ensure resources are released.
      if ( context != null )
        try { context.commitContext(); }
        catch (XythosException ex2) { logger.error( "Exception while committing Xythos context.", ex2 ); };
    }
  }
  
  
  
  
  /**  
   * This is called when this server becomes responsible for monitoring the
   * Xythos content collection. It must connect to the data log file and
   * register with Xythos.
   */
  public void startProcessing()
  {
    if ( monitoringxythos )
      return;
    try
    {
      logger.info( "Starting listening to Xythos." );
      // Process events after commit to database is completed
      // albeit after some delay.
      StorageServerEventBrokerImpl.addAsyncListener( this );
      monitoringxythos = true;
    }
    catch ( Throwable th )
    {
      logger.error( th );
    }
  }

  
  /**
   * Closes the data log file so another server can open it. This de-registers
   * with Xythos.
   */
  public void stopProcessing()
  {
    if ( !monitoringxythos )
      return;
    logger.info( "Stopping listening to Xythos." );
    StorageServerEventBrokerImpl.removeImmedListener( this );    
    monitoringxythos = false;
  }
  
  
  public boolean sendMessage( MessageHeader header, byte[] content )
  {
    Context context = null;
    try
    {
      context = ContextFactory.create( xythosutils.getXythosUser(), new Properties() );
      String id = MessageUtils.createMessageId();
      String mhjson = mhjsonconv.write( header );
      logger.info( "Sending message with this heading" );
      logger.info( mhjson );
      FileSystemFile fsfile = xythosutils.createFile(context, exchangepath, id, "text/plain", content );
      String entryid = fsfile.getEntryID();
      // commit the file creation so that it exists when the
      // coordinator receives a notification of the property addition
      context.commitContext();
      context = ContextFactory.create( xythosutils.getXythosUser(), new Properties() );
      // need to find file again because this is new context.
      FileSystemEntry fse = FileSystem.findEntryFromEntryID( entryid, false, context );
      Property p = fse.addProperty( propdefdestination, mhjson, true, context );
      context.commitContext();
    }
    catch ( Exception ex )
    {
      logger.error( "Exception sending message.", ex );
      if ( context != null )
        try { context.rollbackContext(); }
        catch (XythosException ex1) { logger.error( "Exception rolling back Xythos context.", ex1 ); }
      context = null;
      return false;
    }
    return true;
  }
  
  
  /**
   * Part of the implementation of StorageServerEventListener interface.
   * @return List of event classes we are interested in.
   */
  @Override
  public Class[] listensFor()
  {
    return listensfor;    
  }

  /**
   * Part of the implementation of StorageServerEventListener interface.Receives notification of events.
   * 
   * @throws com.xythos.storageServer.api.VetoEventException
   * Throwing this exception vetoes the event being passed on to other listeners.
   * It does not veto the action that caused the event.
   */
  @Override
  public void processEvent( Context cntxt, FileSystemEvent fse ) throws Exception, VetoEventException
  {
    try
    {
      if ( !(fse instanceof FileSystemEntryPropertyAddedEvent) )
        return;

      FileSystemEntryPropertyAddedEvent fspae = (FileSystemEntryPropertyAddedEvent)fse;
      String path = fspae.getFileSystemEntryName();      
      if ( !path.startsWith( exchangepath ) )
        return;
      if ( !fspae.getPropertyNamespace().equals( XythosUtils.COORDINATION_NAMESPACE ) )
        return;
      if ( !fspae.getPropertyName().equals( rxlocalpropname ) )
        return;
      logger.info( "Detected message file being marked with RXLOCALLY property so process it as incoming." );
      acceptIncomingMessage( fspae.getEntryID() );
    }
    catch ( Exception e )
    {
      logger.error( "Exception while handling file created event.", e );
    }
  }

  final void acceptIncomingMessage( String entryid )
  {
      
    Context context = null;
    try
    {
      context = ContextFactory.create( xythosutils.getXythosUser(), new Properties() );
      FileSystemEntry entry = FileSystem.findEntryFromEntryID( entryid, false, context );
      if ( entry == null )
      {
        logger.error( "Cannot find message file with id " + entryid );
        return;
      }
      if ( !(entry instanceof FileSystemFile) )
      {
        logger.error( "Message with id " + entryid + " is not a file.");
        return;
      }
      FileSystemFile fsf = (FileSystemFile)entry;
      if ( fsf.getSizeOfFileVersion() > 4L*1024L )
      {
        logger.error( "Message with id " + entryid + " is too big.");
        return;
      }
      
      String mhjson = fsf.getProperty( propdefdestination, false, context ).getValue();
      MessageHeader header = mhjsonconv.read( mhjson );
      
      if ( !header.toPlugin.equals( "*" ) && !header.toPlugin.equals( getPluginId() ))
        return;
      
      if ( !header.toServer.equals( "*" ) && !header.toServer.equals( getServerId() ))
        return;
      
      logger.info( " Incoming message has this header: [" + mhjson + "] " );
      
      if ( processMessageHeader( header ) )
        return;
            
      // it's a JSON message of the type we expected.
      ByteArrayOutputStream baout = new ByteArrayOutputStream();
      fsf.getFileContent( baout );
      
      processMessageContent( header, baout.toByteArray() );
    }
    catch ( Exception ex )
    {
      logger.error( "Exception loading message.", ex );
      if ( context != null )
        try { context.rollbackContext(); } catch (XythosException ex1) {logger.error("Exception",ex1);}
      context = null;
    }
    finally
    {
      if ( context != null )
        try { context.commitContext(); } catch (XythosException ex1) {logger.error("Exception",ex1);}
    }    
  }
  
  /**
   * Override to process incoming message headers.
   * @param header The header that was stored in a dav property attached to the file.
   * @return Return true to indicate that the message is now fully processed without accessing the content.
   */
  public abstract boolean processMessageHeader( MessageHeader header );
  
  /**
   * Override to process the content of the messsage.
   * @param header The header that was stored in a dav property attached to the file.
   * @param content The raw content of the message loaded from the file.
   */
  public abstract void processMessageContent( MessageHeader header, byte[] content );
  
  
  /**
   * Part of the implementation of StorageServerEventListener interface.
   * @return In our case there is no sub queue - returns null.
   */
  @Override
  public EventSubQueue getEventSubQueue()
  {
    return null;
  }  

}
