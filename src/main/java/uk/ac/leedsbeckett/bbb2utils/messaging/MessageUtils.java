/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.ac.leedsbeckett.bbb2utils.messaging;

import java.util.Random;

/**
 *
 * @author jon
 */
public class MessageUtils
{
  private static Random random = new Random( System.currentTimeMillis() );
  public static String createMessageId()
  {
    return Long.toHexString(System.currentTimeMillis()) + "_" + Integer.toHexString( random.nextInt() );
  }
}
