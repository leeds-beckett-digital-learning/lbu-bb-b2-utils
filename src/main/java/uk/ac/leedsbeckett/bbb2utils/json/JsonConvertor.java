/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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
