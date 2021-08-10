/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.ac.leedsbeckett.bbb2utils;

/**
 *
 * @author jon
 */
public class CoordinationException extends Exception
{

  public CoordinationException()
  {
    super( "Coordination failure." ); 
  }

  public CoordinationException( String message )
  {
    super( message );
  }

  public CoordinationException(String message, Throwable cause)
  {
    super(message, cause);
  }

  public CoordinationException(Throwable cause)
  {
    super(cause);
  }
  
  
}
