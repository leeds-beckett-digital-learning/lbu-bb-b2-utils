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
package uk.ac.leedsbeckett.bbb2utils.union;

import java.lang.reflect.Field;
import java.util.HashMap;

/**
 *
 * @author jon
 * @param <B>
 */
public abstract class Union<B>
{
  private static final HashMap<String,HashMap<String,Field>> mapoftypemaps = new HashMap<>();  
  private static HashMap<String,Field> getTypeMap( Class currentcontainerclass )
  {
    String canonicalname = currentcontainerclass.getCanonicalName();
    HashMap<String,Field> typemap = mapoftypemaps.get( canonicalname );
    if ( typemap != null ) return typemap;
    typemap = new HashMap<>();
    mapoftypemaps.put( canonicalname, typemap );
    for ( Field f : currentcontainerclass.getDeclaredFields() )
    {
      if ( f.isAnnotationPresent( UnionMember.class ) )
        typemap.put( f.getType().getCanonicalName(), f );
    }
    return typemap;
  }
  
  public String classname;
  
  public void clear()
  {
    HashMap<String,Field> typemap = getTypeMap( getClass() );
    for ( Field f : typemap.values() )
      try { f.set( this, null ); } catch (Exception ex) {}
  }
  
  public void set( B o )
  {
    HashMap<String,Field> typemap = getTypeMap( getClass() );
    clear();
    classname = null;
    if ( o == null ) return;
    String canonicalname = o.getClass().getCanonicalName();
    Field f = typemap.get( canonicalname );
    if ( f == null )
      throw new IllegalArgumentException( "Cannot accept object of class " + o.getClass().getCanonicalName() );
    try { f.set( this, o ); } catch ( Exception ex ) {}
    classname = canonicalname;
  }

  private Object getObject()
  {
    HashMap<String,Field> typemap = getTypeMap( getClass() );
    if ( classname == null ) return null;
    Field f = typemap.get( classname );
    if ( f == null )
      return null;
    Object o = null;
    try { o = f.get( this ); } catch ( Exception ex ) {}
    return o;
  }
  
  public <T extends Object> T  get( Class<T> valueType )
  {
    Object o = getObject();
    if ( o == null ) return null;
    if ( valueType.isInstance(o) )
      return valueType.cast(o);
    return null;
  }
  
  @SuppressWarnings("unchecked")
  public B get()
  {
    Object o = getObject();
    if ( o == null ) return null;
    return (B)o;
  }  
}
