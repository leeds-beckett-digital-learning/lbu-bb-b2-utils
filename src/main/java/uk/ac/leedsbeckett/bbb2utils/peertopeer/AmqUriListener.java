/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.ac.leedsbeckett.bbb2utils.peertopeer;

/**
 *
 * @author jon
 */
public interface AmqUriListener
{
  public void processAmqUriChange( String uri );
}