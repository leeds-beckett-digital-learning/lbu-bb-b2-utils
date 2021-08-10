/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.ac.leedsbeckett.bbb2utils.messaging;

/**
 *
 * @author jon
 */
public class MessageHeader
{
  public enum ContentType
  {
    EMPTY,
    BINARY,
    JSON
  }
  
  public ContentType contentType;
  public String canonicalClassName;
  public String mimeType;
  
  public String fromPlugin;
  public String fromServer;
  public String toPlugin;
  public String toServer;
}
