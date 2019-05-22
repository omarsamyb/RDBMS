package DataBucketsII;

import java.io.Serializable;
import java.util.Vector;

/*
 * Note this class is used to load table
 * into memory otherwise all tables are 
 * saved in the memory and only loaded 
 * when they are needed.
 */
public class Table implements Serializable
{
	private static final long serialVersionUID = 1L;
	Vector<Object []> table; //Each Objects [] is a row
	public Table()
	{
		table=new Vector<Object []>();
	}
}
