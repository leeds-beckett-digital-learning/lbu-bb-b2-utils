/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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
