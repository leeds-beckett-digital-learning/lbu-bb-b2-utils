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
package uk.ac.leedsbeckett.bbb2utils.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

/**
 *
 * @author jon
 * @param <T>
 */
public class JsonConvertor<T>
{
  private static final ObjectMapper objectmapper = new ObjectMapper();
  static
  {
    objectmapper.enable( SerializationFeature.INDENT_OUTPUT );
    objectmapper.disable( SerializationFeature.FAIL_ON_EMPTY_BEANS );
  }

  private final Class<T> c;
  public JsonConvertor( Class<T> c )
  {
    this.c = c;
  }
  
  public T read( String json ) throws JsonProcessingException
  {
    return objectmapper.readValue( json, c );
  }
  
  public T read( Reader reader ) throws JsonProcessingException, IOException
  {
    return objectmapper.readValue( reader, c );
  }
  
  public String write( T o ) throws JsonProcessingException
  {
    return objectmapper.writeValueAsString( o );
  }
  
  public void write( Writer w, T o ) throws JsonProcessingException, IOException
  {
    objectmapper.writeValue(w, o);
  }
}
