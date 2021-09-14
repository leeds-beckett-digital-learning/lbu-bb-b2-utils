/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.ac.leedsbeckett.bbb2utils.peertopeer;

import blackboard.platform.discovery.PeerDiscoveryManager;
import blackboard.platform.discovery.PeerDiscoveryManagerFactory;
import blackboard.platform.discovery.PeerEventListener;
import blackboard.platform.discovery.PeerService;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author jon
 */
public class AmqHelper implements PeerEventListener
{
  public static final String BROKER_SERVICE_ID = "activemq_broker";

  String uri = null;
  
  private final ArrayList<AmqUriListener> listeners = new ArrayList<>();
  
  /**
   * 
   */
  public AmqHelper()
  {
    PeerDiscoveryManager pdm = PeerDiscoveryManagerFactory.getInstance();
    List<PeerService> list = pdm.findPeers( BROKER_SERVICE_ID, true /* include self */ );
    // should only be one
    if ( list != null && list.size() > 0 )
      uri = list.get( 0 ).getPayload();
  }
  
  /**
   * Listens to own cluster node because the active MQ broker may be here.
   * @return 
   */
  @Override
  public boolean listenToSelf()
  {
    return true;
  }

  /**
   * Only interested in activemq broker
   * @param string
   * @return 
   */
  @Override
  public boolean listensToService( String serviceid )
  {
    return BROKER_SERVICE_ID.equals( serviceid );
  }

  
  @Override
  public void peerAdded( PeerService ps )
  {
    if ( !listensToService( ps.getServiceId() ) ) return;  // should never happen but check anyway
    if ( uri != null ) return;
    uri = ps.getPayload();
    tellListeners( uri );
  }

  @Override
  public void peerRemoved( PeerService ps )
  {
    if ( !listensToService( ps.getServiceId() ) ) return;  // should never happen but check anyway
    if ( ps.getPayload().equals( uri ) )
    {
      uri = null;
      tellListeners( null );
    }
  }

  @Override
  public void nodeResumed()
  {
  }
  
  public void tellListeners( String uri )
  {
    AmqUriListener[] a = new AmqUriListener[listeners.size()];
    synchronized ( listeners )
    {
      a = listeners.toArray( a );
    }
    for ( AmqUriListener listener : a )
      listener.processAmqUriChange( uri );
  }
  
  public void registerListener( AmqUriListener listener )
  {
    synchronized ( listeners )
    {
      listeners.add( listener );
    }
    listener.processAmqUriChange( uri );
  }
  
  public void unregisterListener( AmqUriListener listener )
  {
    synchronized ( listeners )
    {
      listeners.remove( listener );
    }
  }
}
