package DataBucketsII;

import java.io.Serializable;
import java.util.ArrayList;

public class IndexPage implements Serializable
{
	private static final long serialVersionUID = 1L;
	ArrayList<Object> uniqueValues;
	ArrayList<ArrayList<Integer>> bitMaps;
	public IndexPage(ArrayList<Object> uniqueValues, ArrayList<ArrayList<Integer>> bitMaps)
	{
		this.uniqueValues=uniqueValues;
		this.bitMaps=bitMaps;
	}
	public String toString()
	{
		String string="";
		for(int i=0;i<uniqueValues.size();i++)
			string+=uniqueValues.get(i)+": "+bitMaps.get(i)+"\n";
		return string;
	}
}
