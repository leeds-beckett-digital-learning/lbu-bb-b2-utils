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
