/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.ac.leedsbeckett.bbb2utils;

import com.xythos.common.api.VirtualServer;
import com.xythos.common.api.XythosException;
import com.xythos.security.api.Context;
import com.xythos.security.api.PrincipalManager;
import com.xythos.security.api.UserBase;
import com.xythos.storageServer.api.CreateDirectoryData;
import com.xythos.storageServer.api.CreateFileData;
import com.xythos.storageServer.api.FileSystem;
import com.xythos.storageServer.api.FileSystemDirectory;
import com.xythos.storageServer.api.FileSystemEntry;
import com.xythos.storageServer.api.FileSystemFile;
import com.xythos.storageServer.properties.api.PropertyDefinition;
import com.xythos.storageServer.properties.api.PropertyDefinitionManager;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.apache.log4j.Logger;

/**
 *
 * @author jon
 */
public class XythosUtils
{
  public static final String COORDINATION_NAMESPACE = "uk.ac.leedsbeckett/coordination";
  public static final String COORDINATION_PROPERTY_DESTINATION = "destination";
  public static final String COORDINATION_PROPERTY_RXLOCALLY = "rxlocally";

  
  VirtualServer xythosvserver;
  UserBase xythosuser;  
  String xythosprincipalid;
  Logger logger;
  
  /**
   * Create a util object that will use the default virtual server and
   * will set the specified user as the owner of created files etc.
   * @param username
   * @throws XythosException 
   */
  public XythosUtils( String username ) throws XythosException
  {
    xythosvserver = VirtualServer.getDefaultVirtualServer();
    for ( String location : PrincipalManager.getUserLocations() )
    {
      xythosuser = PrincipalManager.findUser( username, location );
      if ( xythosuser != null )
        break;
    }
    xythosprincipalid = xythosuser.getPrincipalID();
  }

  /**
   * Get the current logger.
   * @return 
   */
  public Logger getLogger()
  {
    return logger;
  }

  /**
   * Set a logger.  Is null by default.
   * @return 
   */
  public void setLogger(Logger logger)
  {
    this.logger = logger;
  }

  /**
   * Gets the xythos virtual server for this util.
   * @return 
   */
  public VirtualServer getXythosVirtualServer()
  {
    return xythosvserver;
  }

  /**
   * Gets the xythos user for this util.
   * @return 
   */
  public UserBase getXythosUser()
  {
    return xythosuser;
  }

  /** Gets the principal ID of the xythos user for this util
   * 
   * @return 
   */
  public String getXythosPrincipalId()
  {
    return xythosprincipalid;
  }
  
  public PropertyDefinition getOrCreatePropertyDefinition( String name, String description, Context context ) throws XythosException
  {
    PropertyDefinition def = PropertyDefinitionManager.findPropertyDefinition(
            COORDINATION_NAMESPACE, name, context );
    if ( def == null )
      def = PropertyDefinitionManager.createPropertyDefinitionSafe(
              COORDINATION_NAMESPACE, 
              name, 
              PropertyDefinition.DATATYPE_SHORT_STRING, 
              false, // not versioned
              true,  // readable
              true,  // writable
              true,  // case sensitive
              false, // not protected
              false, // not full text indexed
              description );
    return def;
  }

  public FileSystemFile createFile( Context context, String locationpath, String name, String mime, byte[] data ) throws XythosException
  {
    ByteArrayInputStream bain = new ByteArrayInputStream( data );
    return createFile( context, locationpath, name, mime, bain );
  }
  
  public FileSystemFile createFile( Context context, String locationpath, String name, String mime, InputStream in ) throws XythosException
  {
    if ( logger != null )
      logger.debug( "Creating file parent = " + locationpath + "  " + name );
    CreateFileData cfd = new CreateFileData(
            xythosvserver,
            locationpath,
            name,
            mime,
            xythosprincipalid,
            in
    );
    FileSystemFile file = FileSystem.createFile( cfd, context );
    return file;
  }
  
  
  /**
   * Create a directory and its ancestors on the current virtual server
   * with the current user as the owner. Does not commit the context.
   * 
   * @param context
   * @param path
   * @return
   * @throws XythosException 
   */
  public boolean createDirectories( 
          Context context, 
          String path ) 
          throws XythosException
  {
    return createDirectories(xythosvserver, 
                              xythosprincipalid, 
                              context, 
                              path );
  }
  
  public boolean createDirectories( 
          Context context, 
          String[] paths ) 
          throws XythosException
  {
    for ( String path : paths )
      if ( !createDirectories(xythosvserver, 
                              xythosprincipalid, 
                              context, 
                              path ) )
        return false;
    return true;
  }
  
  public boolean createDirectories( 
          String xythosprincipalid, 
          Context context, 
          String path ) 
          throws XythosException
  {
    return createDirectories(xythosvserver, 
                              this.xythosprincipalid, 
                              context, 
                              path );
  }
  
  /**
   * Creates a directory and it's ancestors.Does not commit the context.
   * 
   * @param xythosvserver
   * @param context
   * @param xythosprincipalid
   * @param path
   * @return Success or failure. Fails if a file is in the way or if directories cannot be created.
   * @throws com.xythos.common.api.XythosException
   */
  public boolean createDirectories( 
          VirtualServer xythosvserver, 
          String xythosprincipalid, 
          Context context, 
          String path ) 
          throws XythosException
  {
    if ( xythosvserver == null ) return false;
    if ( xythosprincipalid == null ) return false;
    
    if ( logger != null ) logger.debug( "Creating " + path );
    
    FileSystemEntry dir;
    if ( path.startsWith( "/" ) )
      path = path.substring( 1 );
    String[] pathparts = path.split( "/" );
    if ( logger != null )
      for ( int i=0; i<pathparts.length; i++ )
        logger.debug( "    Split " + i + " " + pathparts[i] );
    
    if ( pathparts.length < 2 )
      return false;
    String parent = "/" + pathparts[0] + "/";
    for ( int i=1; i<pathparts.length; i++ )
    {
      String whole = parent + pathparts[i];
      dir = FileSystem.findEntry( xythosvserver, whole, false, context );
      if ( dir == null )
      {
        if ( logger != null )
          logger.debug( "    Creating in parent " + parent + " directory " + pathparts[i] );
        CreateDirectoryData cdd = new CreateDirectoryData( xythosvserver, parent, pathparts[i], xythosprincipalid );
        dir = FileSystem.createDirectory( cdd, context );
      }
      if ( !(dir instanceof FileSystemDirectory) )
        return false;
      parent = parent + pathparts[i] + "/";
    }    
    return true;
  }
}
