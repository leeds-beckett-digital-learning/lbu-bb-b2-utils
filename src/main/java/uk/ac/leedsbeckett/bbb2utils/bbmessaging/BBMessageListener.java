/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package uk.ac.leedsbeckett.bbb2utils.bbmessaging;

import javax.jms.Message;

/**
 *
 * @author jon
 */
public interface BBMessageListener
{
  public void onMessage( Message msg );
}
