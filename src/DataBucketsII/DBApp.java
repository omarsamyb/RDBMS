package DataBucketsII;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class DBApp 
{
	/*
	 * TODO handle case where there is no given clusteringColumn or multiple clustering Columns
	 * handle case where input is given null
	 * 
	 * 
	 * TODO add properties file
	 */
	public static int maxSizeOfPage=2;
	public static int maxSizeOfIndex=3;
	public static void main (String [] args) throws DBAppException
	{
//		init();
		String strTableName = "Student";
//		Hashtable<String,String> htblColNameType = new Hashtable<String,String>();
//		htblColNameType.put("id", "java.lang.Integer");
//		htblColNameType.put("name", "java.lang.String");
//		htblColNameType.put("gpa", "java.lang.Double");
//		htblColNameType.put("dob", "java.lang.Boolean");
//		createTable(strTableName,"id",htblColNameType);
		
		Hashtable<String,Object> htblColNameValue = new Hashtable<String,Object>();
//		htblColNameValue.put("id", new Integer("0"));
//		htblColNameValue.put("name", new String("Omar"));
//		htblColNameValue.put("gpa", new Double(1.05));
//		htblColNameValue.put("dob", new Boolean(true));
//		insertIntoTable(strTableName, htblColNameValue);
//		
//		htblColNameValue = new Hashtable<String,Object>();
//		htblColNameValue.put("id", new Integer("1"));
//		htblColNameValue.put("name", new String("Shams"));
//		htblColNameValue.put("gpa", new Double(1.11));
//		htblColNameValue.put("dob", new Boolean(true));
//		insertIntoTable(strTableName,htblColNameValue);
//		
//		htblColNameValue = new Hashtable<String,Object>();
//		htblColNameValue.put("id", new Integer("2"));
//		htblColNameValue.put("name", new String("Ahmed"));
//		htblColNameValue.put("gpa", new Double(1.22));
//		htblColNameValue.put("dob", new Boolean(false));
//		insertIntoTable(strTableName,htblColNameValue);
//		
//		htblColNameValue = new Hashtable<String,Object>();
//		htblColNameValue.put("id", new Integer("3"));
//		htblColNameValue.put("name", new String("Mohamed"));
//		htblColNameValue.put("gpa", new Double(1.33));
//		htblColNameValue.put("dob", new Boolean(false));
//		insertIntoTable(strTableName,htblColNameValue);
//		
//		htblColNameValue = new Hashtable<String,Object>();
//		htblColNameValue.put("id", new Integer("4"));
//		htblColNameValue.put("name", new String("Ragb"));
//		htblColNameValue.put("gpa", new Double(1.44));
//		htblColNameValue.put("dob", new Boolean(false));
//		insertIntoTable(strTableName,htblColNameValue);
//		
//		htblColNameValue = new Hashtable<String,Object>();
//		htblColNameValue.put("id", new Integer("5"));
//		htblColNameValue.put("name", new String("Samy"));
//		htblColNameValue.put("gpa", new Double(1.44));
//		htblColNameValue.put("dob", new Boolean(true));
//		insertIntoTable(strTableName,htblColNameValue);
//		
//		htblColNameValue = new Hashtable<String,Object>();
//		htblColNameValue.put("id", new Integer("6"));
//		htblColNameValue.put("name", new String("Manta Triggered"));
//		htblColNameValue.put("gpa", new Double(1.55));
//		htblColNameValue.put("dob", new Boolean(false));
//		insertIntoTable(strTableName,htblColNameValue);
//		
//		htblColNameValue = new Hashtable<String,Object>();
//		htblColNameValue.put("id", new Integer("7"));
//		htblColNameValue.put("name", new String("Manta Very Triggered"));
//		htblColNameValue.put("gpa", new Double(1.55));
//		htblColNameValue.put("dob", new Boolean(true));
//		insertIntoTable(strTableName,htblColNameValue);
//		createBitmapIndex(strTableName, "id");
//		createBitmapIndex(strTableName, "dob");
		
		
		htblColNameValue = new Hashtable<String,Object>();
		htblColNameValue.put("id", new Integer(6));
		htblColNameValue.put("name", new String("Ragb"));
		htblColNameValue.put("dob", new Boolean(true));
		htblColNameValue.put("gpa", new Double(1.44));
//		deleteFromTable(strTableName, htblColNameValue);
		
		htblColNameValue = new Hashtable<String,Object>();
		htblColNameValue.put("name", new String("Omar"));
		htblColNameValue.put("id", new Integer(5));
//		htblColNameValue.put("dob", new Boolean(false));
//		htblColNameValue.put("gpa", new Double(0.998755999));
//		updateTable(strTableName, "5", htblColNameValue);
		
		displayTable(strTableName);
		displayIndex(strTableName, "id");
		displayIndex(strTableName, "name");
		displayIndex(strTableName, "dob");
		displayIndex(strTableName, "gpa");
		
//		int []x=bsInsertUpdate(1, strTableName, "id", 1);
//		System.out.println(x[0]+" "+x[1]);
	
		SQLTerm arrSQLTerms= new SQLTerm();
		arrSQLTerms.strTableName = "Student";
		arrSQLTerms.strColumnName= "id";
		arrSQLTerms.strOperator = "<=";
		arrSQLTerms.objValue = new Integer(0);
//		System.out.println(bs(arrSQLTerms, 1));
		
//		SQLTerm[] arrSQLTerms= new SQLTerm[4];
//		arrSQLTerms[0] = new SQLTerm();
//		arrSQLTerms[0].strTableName = "Student";
//		arrSQLTerms[0].strColumnName= "dob";
//		arrSQLTerms[0].strOperator = "=";
//		arrSQLTerms[0].objValue = new Boolean(true);
//		arrSQLTerms[1] = new SQLTerm();
//		arrSQLTerms[1].strTableName = "Student";
//		arrSQLTerms[1].strColumnName= "id";
//		arrSQLTerms[1].strOperator = "<";
//		arrSQLTerms[1].objValue = new Integer(15);
//		arrSQLTerms[2] = new SQLTerm();
//		arrSQLTerms[2].strTableName = "Student";
//		arrSQLTerms[2].strColumnName= "name";
//		arrSQLTerms[2].strOperator = "=";
//		arrSQLTerms[2].objValue = new String("f");
//		arrSQLTerms[3] = new SQLTerm();
//		arrSQLTerms[3].strTableName = "Student";
//		arrSQLTerms[3].strColumnName= "gpa";
//		arrSQLTerms[3].strOperator = "<";
//		arrSQLTerms[3].objValue = new Double(0);
//		String[]strarrOperators = new String[3];
//		strarrOperators[0]="AND";
//		strarrOperators[1]="OR";
//		strarrOperators[2]="OR";
//	
//		Iterator<Object[]> query = selectFromTable(arrSQLTerms, strarrOperators);
//		if(!query.hasNext())
//			System.out.println("No results found");
//		while(query.hasNext())
//		{
//			Object[] x = (Object[]) query.next();
//			if(x==null)
//				break;
//			for(int i=0;i<x.length;i++)
//			{
//				if(i==x.length-1)
//					System.out.print(x[i]);
//				else
//					System.out.print(x[i]+",");
//			}
//			System.out.println();
//		}	
		
//		htblColNameValue.put("id", new Integer(1));
//		htblColNameValue.put("name", new String("Shams"));
//		htblColNameValue.put("dob", new Date("3/24/2019"));
//		htblColNameValue.put("isMarried", new Boolean(true));
//		htblColNameValue.put("gpa", new Double(0.998755999));
//		deleteFromTable(strTableName, htblColNameValue);
	}
	
	//Requested Methods
	public static void init() throws DBAppException
	{
		String initMetaData="Table Name,Column Name,Column Type,Key,Indexed"+"\n";
		try 
		{
			PrintWriter pw=new PrintWriter(new FileWriter(new File("./data/metadata.csv")));
			pw.print(initMetaData);
			pw.close();
			
			@SuppressWarnings("resource")
			BufferedReader bf=new BufferedReader(new FileReader("./config/DBApp.properties"));
			while(bf.ready())
			{
				StringTokenizer st=new StringTokenizer(bf.readLine());
				if(st.countTokens()!=3)
					throw new DBAppException("Invalid syntax of DBApp.properties");
				String data=st.nextToken();	st.nextToken();
				if(data.equals("MaximumRowsCountinPage"))
					maxSizeOfPage=Integer.parseInt(st.nextToken());
				else if(data.equals("BitmapSize"))
					maxSizeOfIndex=Integer.parseInt(st.nextToken());	
			}
			bf.close();
		}catch (IOException e) {e.printStackTrace(); }
	}
	public static void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType )throws DBAppException
	{
		FileReader reader = null;
		try 
		{
			reader=new FileReader("./data/metadata.csv");
			BufferedReader bf=new BufferedReader(reader);
			while(bf.ready())
			{
				StringTokenizer st=new StringTokenizer(bf.readLine(),",");
				if(st.nextToken().equals(strTableName))
				{
					bf.close();
					throw new DBAppException("This table already exits.");
				}
			}
			bf.close();
		} 
		catch (IOException e) {e.printStackTrace();}
		
		String tableName=strTableName;
		String [] colNames=new String [1+htblColNameType.size()];
		String [] dataTypes=new String [1+htblColNameType.size()];
		boolean []isIndexed=new boolean [1+htblColNameType.size()]; //Note: cannot index on col 0 however it starts from 0 for convinent use
		colNames [0]="TouchDate";
		dataTypes[0]="java.util.Date";
		int i=1;
		for(String key: htblColNameType.keySet())
		{
			String dataType=htblColNameType.get(key);
			String columnName=key;
			boolean a=dataType.equals("java.lang.Integer");
			boolean b=dataType.equals("java.lang.String");
			boolean c=dataType.equals("java.lang.Double");
			boolean d=dataType.equals("java.lang.Boolean");
			boolean e=dataType.equals("java.util.Date");
			if(!a && !b && !c && !d && !e)
				throw new DBAppException("Invalid data type "+dataType);
			colNames[i]=columnName;
			dataTypes[i++]=dataType;
        }

		try 
		{
			reader=new FileReader("./data/metadata.csv");
			BufferedReader bf=new BufferedReader(reader);
			String line="";
			while(bf.ready())
				line+=bf.readLine()+"\n";
			for(int j=colNames.length-1;j>0;j--)
				line+=tableName+","+colNames[j]+","+dataTypes[j]+","+(strClusteringKeyColumn.equals(colNames[j])? "true":"false")+","+isIndexed[j]+"\n";
			FileWriter fw=new FileWriter(new File("./data/metadata.csv"));
			PrintWriter pw=new PrintWriter(fw);
			pw.print(line);
			pw.close();
			bf.close();
		} 
		catch (Exception e) {e.printStackTrace();}
	}
	public static void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException
	{
		Object [] toBeInserted=toObjectArray(strTableName, htblColNameValue);
		int [] place=findPlace(strTableName, toBeInserted);
		insertAndShiftDown(strTableName, place, toBeInserted);
		
		int bitNumber=getLocationOf(strTableName,place);
		ArrayList<String []> metaData=getRelatedMetaData(strTableName);
		for(int i=1;i<metaData.size();i++)
		{
			if(metaData.get(i)[4].equals("true"))
			{
				String strColName=metaData.get(i)[1];
				int[] pageAndLocation=getUniqueValueLocation(strTableName, strColName, toBeInserted[i]);
				insertValueIntoIndex(strTableName, strColName, pageAndLocation, bitNumber, toBeInserted[i]);
			}
		}
	}
	public static void updateTable(String strTableName,String strKey,Hashtable<String,Object>htblColNameValue)throws DBAppException
	{
		ArrayList<String[]> relatedMetaData=getRelatedMetaData(strTableName);
		int keyindex= getKeyIndex(relatedMetaData);
		Object[] toBeInserted=toObjectArray(strTableName, htblColNameValue);
		if(toBeInserted[keyindex]!=null)
			updateClustring(strTableName,strKey,htblColNameValue);
		else
			updateNonClustring(strTableName,strKey,htblColNameValue);
		
			for(String colName:htblColNameValue.keySet())
				removeEmptyBitMaps(strTableName, colName);
			
	}
	public static void deleteFromTable(String strTableName,Hashtable<String,Object> htblColNameValue) throws DBAppException
	{
		ArrayList<String> indices = new ArrayList<String>();
		boolean[]whichFieldHasIndex = new boolean[htblColNameValue.size()];
		BitSet[] indicesBitMaps = new BitSet[htblColNameValue.size()];
		ArrayList<String> orderedColumns= new ArrayList<String>();
		boolean indexed=false;
		// get Column names and its corresponding data from input
		String [] colNames=new String [htblColNameValue.size()];
		Object [] colData=new Object [htblColNameValue.size()];
		boolean [] colFound = new boolean [htblColNameValue.size()];
		int fck = 0;
		for(String key: htblColNameValue.keySet())
		{
			Object data=htblColNameValue.get(key);
			String colName=key;
			colNames[fck]=colName;
			colData[fck++]=data;
        }
		
		// check strTableName is correct & ColumnName-Object type is correct combination
		try 
		{
			boolean found=false;
			BufferedReader br=new BufferedReader(new FileReader("./data/metadata.csv"));
			A:while(br.ready())
			{
				StringTokenizer st=new StringTokenizer(br.readLine(),",");
				if(st.nextToken().equals(strTableName))
				{
					found=true;
					String colName = st.nextToken();
					String colType = st.nextToken();
					st.nextToken();
					String colIndexed= st.nextToken();
					if(colIndexed.equals("true"))
						indices.add(colName);
					for(int j =0;j<colNames.length;j++)
					{
						if(colName.equals(colNames[j]))
						{
							if(!compareTypes(colType,colData[j]))
							{
								break A;
							}
							colFound[j]=true;
							if(colIndexed.equals("true"))
							{
								whichFieldHasIndex[j]=true;
								indexed=true;
							}
						}
					}
					orderedColumns.add(colName);
				}
				else
				{
					if(found==true)
						break;
				}
			}
			br.close();
			if(!found)
				throw new DBAppException("table not found");
			for(int j =0;j<colFound.length;j++)
			{
				if(!colFound[j])
				{
					throw new DBAppException("invalid column names/types");
				}
			}
		}
		catch (FileNotFoundException e) 
		{
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		if(indexed)
		{
			for(int i=0;i<whichFieldHasIndex.length;i++)
			{
				BitSet mybits=null;
				if(whichFieldHasIndex[i]==true)
				{
					IndexPage indexPage=null;
					if(colData[i] instanceof Boolean)
					{
						for(int j=0;new File("./data/"+strTableName+"-"+colNames[i]+"-"+j).exists();j++)
						{
							indexPage = readCompressed(strTableName, colNames[i], j);
							for(int k=0;k<indexPage.uniqueValues.size();k++)
							{
								if(evaluate(indexPage.uniqueValues.get(k), "=", colData[i]))
								{
									mybits=convertToBitSet(indexPage.bitMaps.get(k));
									break;
								}
							}
						}
						if(mybits==null)
						{
							mybits=new BitSet();
						}
						indicesBitMaps[i]=mybits;
					}
					else
					{
						final String colname = colNames[i];
						File dir = new File("./data/");
						File[] foundFiles = dir.listFiles(new FilenameFilter() {
						    public boolean accept(File dir, String name) {
						        return name.startsWith(strTableName+"-"+colname+"-"); //&& name.indexOf("-",arrSQLTerms[0].strTableName.length()+1)!=-1;
						    }
						});
						SQLTerm xx = new SQLTerm();
						xx.objValue=colData[i];
						xx.strOperator="=";
						xx.strTableName=strTableName;
						xx.strColumnName=colNames[i];
						mybits=bs(xx, foundFiles.length-1);
						indicesBitMaps[i]=mybits;
					}
				}
			}
			BitSet bitMap=null;
			for(int triggered=0;triggered<indicesBitMaps.length;triggered++)
			{
				if(indicesBitMaps[triggered] != null)
				{
					if(bitMap==null)
						bitMap=indicesBitMaps[triggered];
					else
						bitMap.and(indicesBitMaps[triggered]);
				}
			}
			int counter=0;
			for (int i = bitMap.nextSetBit(0); i >= 0; i = bitMap.nextSetBit(i+1)) 
			{
				int correctIndex = i-counter;
				Vector<Object []> page=null;
				Table x=null;
			    int posInPage= correctIndex%maxSizeOfPage;
				int pageNumber= (int) Math.floor(correctIndex/maxSizeOfPage);
				Object[] tuple=null;
				try
				{
					ObjectInputStream in = new ObjectInputStream(new FileInputStream("./data/"+strTableName+"-"+pageNumber));
					x = (Table) in.readObject();
					page = x.table;
					in.close();
					tuple=page.get(posInPage);
				}
				catch (Exception e) 
				{
			         e.printStackTrace();
				}
				for(int j=0;j<htblColNameValue.size();j++)
			    {
					if(!whichFieldHasIndex[j])
					{
						if(!evaluate(tuple[orderedColumns.indexOf(colNames[j])+1], "=", htblColNameValue.get(colNames[j])))
						{
							bitMap.clear(i);
							break;
						}
					}
			    }
				if(bitMap.get(i))
				{
					Object[]shit=null;
					page.remove(posInPage);
					x.table=page;
					writeTable(strTableName, pageNumber, x);
					while(new File("./data/"+strTableName+"-"+(++pageNumber)).exists())
					{
						x=readTable(strTableName, pageNumber);
						page=x.table;
						shit=page.get(0);
						page.remove(0);
						x.table=page;
						writeTable(strTableName, pageNumber, x);
						x=readTable(strTableName, pageNumber-1);
						page=x.table;
						page.add(shit);
						writeTable(strTableName, pageNumber-1, x);
					}
					if(readTable(strTableName, --pageNumber).table.size()==0)
					{
						new File("./data/"+strTableName+"-"+pageNumber).delete();
					}
					for(int k = 0 ; k<indices.size();k++)
					{
						for(int l=0;new File("./data/"+strTableName+"-"+indices.get(k)+"-"+l).exists();l++)
						{
							IndexPage bullshit=readCompressed(strTableName, indices.get(k), l);
							for(int m=0;m<bullshit.uniqueValues.size();m++)
							{
								int deletedBit=bullshit.bitMaps.get(m).remove(correctIndex);
								if(deletedBit==1 && !bullshit.bitMaps.get(m).contains(1))
								{
									bullshit.uniqueValues.remove(m);
									bullshit.bitMaps.remove(m);
									if(bullshit.uniqueValues.size()==0)
									{
										new File("./data/"+strTableName+"-"+indices.get(k)+"-"+l).delete();
										for(int n=l+1;new File("./data/"+strTableName+"-"+indices.get(k)+"-"+n).exists();n++)
										{
											File newName=new File("./data/"+strTableName+"-"+indices.get(k)+"-"+(n-1));
											new File("./data/"+strTableName+"-"+indices.get(k)+"-"+n).renameTo(newName);
										}
										l--;
										break;
									}
									else
										m--;
								}
							}
							if(bullshit.uniqueValues.size()!=0)
								writeCompressed(strTableName, indices.get(k), l, bullshit);
						}
					}
					counter++;
				}
			}		
		}
		else
		{
			int counter=0;
			int i=0;
			for(i=0;new File("./data/"+strTableName+"-"+i).exists();i++)
			{
				try 
				{
					Vector<Object []> page;
					ObjectInputStream in = new ObjectInputStream(new FileInputStream("./data/"+strTableName+"-"+i));
			        Table fckoff = (Table) in.readObject();
			        page=fckoff.table;
			        in.close();
			        for(int j=0;j<page.size();j++)
			        {
			        	boolean andResult=false;
			        	for(int k=0;k<colNames.length;k++)
			        	{
			    			int index = orderedColumns.indexOf(colNames[k]);
			    			index++;
			    			if(evaluate(page.get(j)[index], "=", colData[k]))
				        	{
			    				andResult=true;
				        	}
			    			else
			    			{
			    				andResult=false;
			    				break;
			    			}
			        	}
			        	if(andResult)
			        	{
			        		Object[]shit=null;
							page.remove(j);
							fckoff.table=page;
							writeTable(strTableName, i, fckoff);
							j--;
							int subi=i+1;
							while(new File("./data/"+strTableName+"-"+subi).exists())
							{
								fckoff=readTable(strTableName, subi);
								page=fckoff.table;
								shit=page.get(0);
								page.remove(0);
								fckoff.table=page;
								writeTable(strTableName, subi, fckoff);
								if(fckoff.table.size()==0) 
						        {
									new File("./data/"+strTableName+"-"+(subi)).delete();
						        }
								fckoff=readTable(strTableName,subi-1);
								page=fckoff.table;
								page.add(shit);
								writeTable(strTableName, subi-1, fckoff);
								subi++;
							}
							fckoff=readTable(strTableName, i);
							page=fckoff.table;
							for(int k = 0 ; k<indices.size();k++)
							{
								for(int l=0;new File("./data/"+strTableName+"-"+indices.get(k)+"-"+l).exists();l++)
								{
									IndexPage bullshit=readCompressed(strTableName, indices.get(k), l);
									for(int m=0;m<bullshit.uniqueValues.size();m++)
									{
										int correctIndex = (maxSizeOfPage*i+(j+1))-counter;
										int deletedBit=bullshit.bitMaps.get(m).remove(correctIndex);
										if(deletedBit==1 && !bullshit.bitMaps.get(m).contains(1))
										{
											bullshit.uniqueValues.remove(m);
											bullshit.bitMaps.remove(m);
											if(bullshit.uniqueValues.size()==0)
											{
												new File("./data/"+strTableName+"-"+indices.get(k)+"-"+l).delete();
												for(int n=l+1;new File("./data/"+strTableName+"-"+indices.get(k)+"-"+n).exists();n++)
												{
													File newName=new File("./data/"+strTableName+"-"+indices.get(k)+"-"+(n-1));
													new File("./data/"+strTableName+"-"+indices.get(k)+"-"+n).renameTo(newName);
												}
												l--;
												break;
											}
											else
												m--;
										}
									}
									if(bullshit.uniqueValues.size()!=0)
										writeCompressed(strTableName, indices.get(k), l, bullshit);
								}
							}
			        	}
			        }
				}
				catch (Exception e) 
				{
			         e.printStackTrace();
				}
			}
			if(new File("./data/"+strTableName+"-"+(i-1)).exists() && readTable(strTableName,i-1).table.size()==0) 
	        {
				new File("./data/"+strTableName+"-"+(i-1)).delete();
	        }
		}
	}
	public static void createBitmapIndex(String strTableName,String strColName) throws DBAppException
	{
		ArrayList<String []> metaData=new ArrayList<String [] >();
		//Checking for exceptions
		int ind=-1;
		int firstcol=-1;
		try 
		{
			FileReader reader=new FileReader("./data/metadata.csv");
			BufferedReader bf=new BufferedReader(reader);
			bf.readLine();
			for(int i=0;bf.ready();i++)
			{
				String [] myArr=new String[5];
				StringTokenizer stringTokenizer=new StringTokenizer(bf.readLine(),",");
				for(int j=0;stringTokenizer.hasMoreTokens();j++)
					myArr[j]=stringTokenizer.nextToken();
				String name=myArr[0];
				if(firstcol==-1 && name.equals(strTableName)) {
					firstcol=i;
				}
				if(name.equals(strTableName) && myArr[1].equals(strColName))
					ind=i;
				metaData.add(myArr);
			}
			bf.close();
			if(ind==-1)
				throw new DBAppException("This table/column doesn't exits");
			if(metaData.get(ind)[4].equals("true"))
				throw new DBAppException("An index is already created on this column");
			metaData.get(ind)[4]="true";
			updateMetaData(metaData);
		} 
		catch (IOException e) {e.printStackTrace();}
		//End of checking for exceptions
		
		int counter=0;
		for(int i=0;new File("./data/"+strTableName+"-"+i).exists();i++)
		{
			Table myTable=readTable(strTableName, i);
			for(Object [] j:myTable.table)
			{
				int[] index=getUniqueValueLocation(strTableName, strColName, j[ind-firstcol+1]);
				insertValueIntoIndex(strTableName,strColName,index,counter++,j[ind-firstcol+1]);
			}
		}
	}
	public static Iterator<Object[]> selectFromTable(SQLTerm[] arrSQLTerms,String[] strarrOperators) throws DBAppException
	{
		ArrayList<Object[]> resultSet= new ArrayList<Object[]>();
		Iterator<Object[]> it;
		ArrayList<String> orderedColumns= new ArrayList<String>();
		ArrayList<String> colTypes= new ArrayList<String>();
		try 
		{
			BufferedReader br=new BufferedReader(new FileReader("./data/metadata.csv"));
			while(br.ready())
			{
				StringTokenizer st=new StringTokenizer(br.readLine(),",");
				if(st.nextToken().equals(arrSQLTerms[0].strTableName))
				{
					String colName = st.nextToken();
					String colType = st.nextToken();
					orderedColumns.add(colName);
					colTypes.add(colType);
				}
			}
			br.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}		
		boolean[]whichFieldHasIndex = new boolean[arrSQLTerms.length];
		boolean indexed=false;
		for(int i=0;i<arrSQLTerms.length;i++)
		{
			if(new File("./data/"+arrSQLTerms[i].strTableName+"-"+arrSQLTerms[i].strColumnName+"-"+0).exists())
			{
				whichFieldHasIndex[i]=true;
				indexed=true;
			}
			if(!new File("./data/"+arrSQLTerms[i].strTableName+"-"+0).exists())
				throw new DBAppException("invalid Table name");
			if(!orderedColumns.contains(arrSQLTerms[i].strColumnName))
				throw new DBAppException("invalid column names");
			if(!compareTypes(colTypes.get(orderedColumns.indexOf(arrSQLTerms[i].strColumnName)), arrSQLTerms[i].objValue))
				throw new DBAppException("invalid column types");
		}
		if(indexed)
		{
			boolean[] flag = new boolean[arrSQLTerms.length];
    		BitSet[] result= new BitSet[arrSQLTerms.length];
    		BitSet[] indicesBitMaps = new BitSet[arrSQLTerms.length];
			for(int i=0;i<whichFieldHasIndex.length;i++)
			{
				if(whichFieldHasIndex[i]==true)
				{
					IndexPage indexPage=null;
					BitSet bitMap=null;
					if(arrSQLTerms[i].strOperator.equals("!=") || arrSQLTerms[i].objValue instanceof Boolean)
					{
						for(int j=0;new File("./data/"+arrSQLTerms[i].strTableName+"-"+arrSQLTerms[i].strColumnName+"-"+j).exists();j++)
						{
							indexPage = readCompressed(arrSQLTerms[i].strTableName, arrSQLTerms[i].strColumnName, j);
							for(int k=0;k<indexPage.uniqueValues.size();k++)
							{
								if(evaluate(indexPage.uniqueValues.get(k), arrSQLTerms[i].strOperator, arrSQLTerms[i].objValue))
								{
									if(bitMap==null)
									{
										bitMap=convertToBitSet(indexPage.bitMaps.get(k));
									}
									else
									{
										bitMap.or(convertToBitSet(indexPage.bitMaps.get(k)));
									}
								}
							}
						}
						if(bitMap==null)
						{
							bitMap=new BitSet();
						}
						indicesBitMaps[i]=bitMap;
					}
					else
					{
						final String colname = arrSQLTerms[i].strColumnName;
						File dir = new File("./data/");
						File[] foundFiles = dir.listFiles(new FilenameFilter() {
						    public boolean accept(File dir, String name) {
						        return name.startsWith(arrSQLTerms[0].strTableName+"-"+colname+"-"); //&& name.indexOf("-",arrSQLTerms[0].strTableName.length()+1)!=-1;
						    }
						});
						bitMap=bs(arrSQLTerms[i], foundFiles.length-1);
						indicesBitMaps[i]=bitMap;
					}
				}
			}
			if(strarrOperators.length>0)
			{
				boolean entered=false;
				BitSet bitwiseOpResult=null;
				// AND with no interrupted precedence
				for(int i=0;i<whichFieldHasIndex.length;i++)
				{
					if(whichFieldHasIndex[i])
					{
						if(i>=strarrOperators.length)
						{
							if(!strarrOperators[i-1].equals("AND"))
								break;
						}
						else
						{
							if(!strarrOperators[i].equals("AND"))
								break;
						}		
						//GO LEFT
						int index=i;
						while(index>0)
						{
							index--;
							BitSet operand1;
							BitSet operand2;
							if(strarrOperators[index].equals("AND") && result[index]==null && !whichFieldHasIndex[index])
							{
								if(result[i]==null)
									result[i]=indicesBitMaps[i];
								int posInArray = orderedColumns.indexOf(arrSQLTerms[index].strColumnName);
        		    			posInArray++;
								for(int j=0;j<result[i].size();j++)
								{
									if(result[i].get(j))
									{
										Object[] myTuple = getTupleFromBitMap(arrSQLTerms[index].strTableName, j);
										if(myTuple==null)
											break;
										if(!evaluate(myTuple[posInArray],arrSQLTerms[index].strOperator,arrSQLTerms[index].objValue))
										{
											result[i].flip(j);
										}
									}
								}
								bitwiseOpResult=result[i];
								result[index]=result[i];
								flag[i]=true;
	            				flag[index]=true;
	            				entered=true;
							}
							else if(strarrOperators[index].equals("AND") && result[index]==null && whichFieldHasIndex[index])
							{
								if(result[i]==null)
									result[i]=indicesBitMaps[i];
								operand1=result[i];
								operand2=indicesBitMaps[index];
								operand1.and(operand2);
								bitwiseOpResult=operand1;
								flag[i]=true;
	            				flag[index]=true;
	            				result[i]=operand1;;
	            				result[index]=operand1;	
	            				entered=true;
							}
							else if(strarrOperators[index].equals("AND") && result[index]!=null)
							{
								if(result[i]==null)
									result[i]=indicesBitMaps[i];
								operand1=result[i];
								operand2=result[index];
								operand1.and(operand2);
								bitwiseOpResult=operand1;
								flag[i]=true;
	            				flag[index]=true;
	            				result[i]=operand1;;
	            				result[index]=operand1;	
	            				entered=true;
							}
							else
							{
								if(entered)
								{
									for(int j=index+2;j<=i;j++)
									{
										result[j]=result[index+1];
									}
									bitwiseOpResult=result[index+1];
									break;
								}
							}
						}
						// GO RIGHT
						index=i;
						entered=false;
						while(index<arrSQLTerms.length)
						{
							index++;
							BitSet operand1;
							BitSet operand2;
							if(index==arrSQLTerms.length)
								break;
							if(strarrOperators[index-1].equals("AND") && result[index]==null && !whichFieldHasIndex[index])
							{
								if(result[i]==null)
									result[i]=indicesBitMaps[i];
								int posInArray = orderedColumns.indexOf(arrSQLTerms[index].strColumnName);
        		    			posInArray++;
								for(int j=0;j<result[i].size();j++)
								{
									Object[] myTuple = getTupleFromBitMap(arrSQLTerms[index].strTableName, j);
									if(myTuple==null)
										break;
									if(result[i].get(j))
									{
										if(!evaluate(myTuple[posInArray],arrSQLTerms[index].strOperator,arrSQLTerms[index].objValue))
										{
											result[i].flip(j);
										}
									}
								}
								bitwiseOpResult=result[i];
								result[index]=result[i];
								flag[i]=true;
	            				flag[index]=true;
	            				entered=true;
							}
							else if(strarrOperators[index-1].equals("AND") && result[index]==null && whichFieldHasIndex[index])
							{
								if(result[i]==null)
									result[i]=indicesBitMaps[i];
								operand1=result[i];
								operand2=indicesBitMaps[index];
								operand1.and(operand2);
								bitwiseOpResult=operand1;
								flag[i]=true;
	            				flag[index]=true;
	            				result[i]=operand1;;
	            				result[index]=operand1;	
	            				entered=true;
							}
							else if(strarrOperators[index-1].equals("AND") && result[index]!=null)
							{
								if(result[i]==null)
									result[i]=indicesBitMaps[i];
								operand1=result[i];
								operand2=result[index];
								operand1.and(operand2);
								bitwiseOpResult=operand1;
								flag[i]=true;
	            				flag[index]=true;
	            				result[i]=operand1;;
	            				result[index]=operand1;	
	            				entered=true;
							}
							else
							{
								//set all values from index to i to be same result
								if(entered)
								{
									for(int j=i;j<index-1;j++)
									{
										result[j]=result[index-1];
									}
									bitwiseOpResult=result[index-1];
									break;
								}
							}
						}
					}
				}
				// for the rest of the unresolved ANDs
				for(int i=0;i<strarrOperators.length;i++)
	        	{
        			if(strarrOperators[i].equals("AND"))
        			{
        				if(result[i]==null || result[i+1]==null)
        				{
        					BitSet operand1;
            				BitSet operand2;
            				if(result[i]!=null && result[i+1]==null)
            				{
            					operand1=result[i];
            					for (int j = operand1.nextSetBit(0); j >= 0; j = operand1.nextSetBit(j+1)) {
            						Object[] myTuple = getTupleFromBitMap(arrSQLTerms[0].strTableName, j);
            						int index = orderedColumns.indexOf(arrSQLTerms[i+1].strColumnName);
            						index++;
            						if(!evaluate(myTuple[index],arrSQLTerms[i+1].strOperator, arrSQLTerms[i+1].objValue))
            						{
            							operand1.clear(j);
            						}
            					 }
                				bitwiseOpResult=operand1;
                				flag[i]=true;
                				flag[i+1]=true;
                				result[i]=operand1;
                				result[i+1]=operand1;
                				continue;
            				}
            				else if(result[i]==null && result[i+1]!=null)
            				{
            					operand2=result[i+1];
            					for (int j = operand2.nextSetBit(0); j >= 0; j = operand2.nextSetBit(j+1)) {
            						Object[] myTuple = getTupleFromBitMap(arrSQLTerms[0].strTableName, j);
            						int index = orderedColumns.indexOf(arrSQLTerms[i].strColumnName);
            						index++;
            						if(!evaluate(myTuple[index],arrSQLTerms[i].strOperator, arrSQLTerms[i].objValue))
            						{
            							operand2.clear(j);
            						}
            					 }
                				bitwiseOpResult=operand2;
                				flag[i]=true;
                				flag[i+1]=true;
                				result[i]=operand2;;
                				result[i+1]=operand2;
                				continue;
            				}
            				else {
            					int index = orderedColumns.indexOf(arrSQLTerms[i].strColumnName);
        		    			index++;
        		    			int index2 = orderedColumns.indexOf(arrSQLTerms[i+1].strColumnName);
        		    			index2++;
        		    			BitSet bitMap=new BitSet();
        		    			int counter=0;
            					for(int j=0;new File("./data/"+arrSQLTerms[i].strTableName+"-"+j).exists();j++)
            					{
            						try
            						{
            							Vector<Object []> page;
            							ObjectInputStream in = new ObjectInputStream(new FileInputStream("./data/"+arrSQLTerms[i].strTableName+"-"+j));
            							Table x = (Table) in.readObject();
            							page = x.table;
            							in.close();
            					        for(int k=0;k<page.size();k++)
            					        {
            					        	if(evaluate(page.get(k)[index], arrSQLTerms[i].strOperator, arrSQLTerms[i].objValue))
            					        	{
            					        		if(evaluate(page.get(k)[index2], arrSQLTerms[i+1].strOperator, arrSQLTerms[i+1].objValue))
            					        		{
            					        			bitMap.set(counter);
            					        		}
            					        		counter++;
            					        	}
            					        	else
            					        		counter++;
            					        }
            						}
            						catch (Exception e) 
            						{
            					         e.printStackTrace();
            						}
            					}
            					bitwiseOpResult=bitMap;
            					operand1=bitMap;
            					operand2=bitMap;
            					result[i]=operand1;
            					result[i+1]=bitMap;
            					flag[i]=true;
            					flag[i+1]=true;
            				}
            				/*
            				if(result[i+1]!=null)
            					operand2=result[i+1];
            				else if(result[i]!=null && result[i+1]==null)
            				{
            					operand1=result[i];
            					for (int j = operand1.nextSetBit(0); j >= 0; j = operand1.nextSetBit(j+1)) {
            						Object[] myTuple = getTupleFromBitMap(arrSQLTerms[0].strTableName, j);
            						int index = orderedColumns.indexOf(arrSQLTerms[i+1].strColumnName);
            						index++;
            						if(!evaluate(myTuple[index],arrSQLTerms[i+1].strOperator, arrSQLTerms[i+1].objValue))
            						{
            							operand1.clear(j);
            						}
            					 }
                				bitwiseOpResult=operand1;
                				flag[i]=true;
                				flag[i+1]=true;
                				result[i]=operand1;;
                				result[i+1]=operand1;
                				continue;
            				}
            				else
            				{
	    						int index = orderedColumns.indexOf(arrSQLTerms[i+1].strColumnName);
	    		    			index++;
	    		    			BitSet bitMap=new BitSet();
	    		    			int counter=0;
	        					for(int j=0;new File("./data/"+arrSQLTerms[i+1].strTableName+"-"+j).exists();j++)
	        					{
	        						try
	        						{
	        							Vector<Object []> page;
	        							ObjectInputStream in = new ObjectInputStream(new FileInputStream("./data/"+arrSQLTerms[i+1].strTableName+"-"+j));
	        							Table x = (Table) in.readObject();
	        							page = x.table;
	        							in.close();
	        					        for(int k=0;k<page.size();k++)
	        					        {
	        					        	if(evaluate(page.get(k)[index], arrSQLTerms[i+1].strOperator, arrSQLTerms[i+1].objValue))
	        					        	{
	        					        		bitMap.set(counter);
	        					        		counter++;
	        					        	}
	        					        	else
	        					        		counter++;
	        					        }
	        						}
	        						catch (Exception e) 
	        						{
	        					         e.printStackTrace();
	        						}
	        					}
	        					operand2=bitMap;
	        					result[i+1]=operand2;
	        					flag[i+1]=true;
	        				}
            				operand1.and(operand2);
            				bitwiseOpResult=operand1;
            				flag[i]=true;
            				flag[i+1]=true;
            				result[i]=operand1;;
            				result[i+1]=operand1;
            				*/
        				}
        			}
	        	}
				// XOR with no interrupted precedence
				entered=false;
				for(int i=0;i<whichFieldHasIndex.length;i++)
				{
					if(whichFieldHasIndex[i])
					{
						if(i>=strarrOperators.length)
						{
							if(!strarrOperators[i-1].equals("XOR"))
								break;
						}
						else
						{
							if(!strarrOperators[i].equals("XOR"))
								break;
						}
						//GO LEFT
						int index=i;
						while(index>0)
						{
							index--;
							BitSet operand1;
							BitSet operand2;
							if(strarrOperators[index].equals("XOR") && result[index]==null && !whichFieldHasIndex[index])
							{
								if(result[i]==null)
									result[i]=indicesBitMaps[i];
								int posInArray = orderedColumns.indexOf(arrSQLTerms[index].strColumnName);
        		    			posInArray++;
								for(int j=0;j<result[i].size();j++)
								{
									Object[] myTuple = getTupleFromBitMap(arrSQLTerms[index].strTableName, j);
									if(myTuple==null)
										break;
									if(result[i].get(j))
									{
										if(evaluate(myTuple[posInArray],arrSQLTerms[index].strOperator,arrSQLTerms[index].objValue))
										{
											result[i].flip(j);
										}
									}
									else
									{
										if(evaluate(getTupleFromBitMap(arrSQLTerms[index].strTableName, j)[posInArray],arrSQLTerms[index].strOperator,arrSQLTerms[index].objValue))
										{
											result[i].flip(j);
										}
									}
								}
								bitwiseOpResult=result[i];
								result[index]=result[i];
								flag[i]=true;
	            				flag[index]=true;
	            				entered=true;
							}
							else if(strarrOperators[index].equals("XOR") && result[index]==null && whichFieldHasIndex[index])
							{
								if(result[i]==null)
									result[i]=indicesBitMaps[i];
								operand1=result[i];
								operand2=indicesBitMaps[index];
								operand1.xor(operand2);
								bitwiseOpResult=operand1;
								flag[i]=true;
	            				flag[index]=true;
	            				result[i]=operand1;;
	            				result[index]=operand1;	
	            				entered=true;
							}
							else if(strarrOperators[index].equals("XOR") && result[index]!=null)
							{
								if(result[i]==null)
									result[i]=indicesBitMaps[i];
								operand1=result[i];
								operand2=result[index];
								operand1.xor(operand2);
								bitwiseOpResult=operand1;
								flag[i]=true;
	            				flag[index]=true;
	            				result[i]=operand1;;
	            				result[index]=operand1;	
	            				entered=true;
							}
							else
							{
								if(entered)
								{
									for(int j=index+2;j<=i;j++)
									{
										result[j]=result[index+1];
									}
									bitwiseOpResult=result[index+1];
									break;
								}
							}
						}
						// GO RIGHT
						index=i;
						entered=false;
						while(index<arrSQLTerms.length)
						{
							index++;
							BitSet operand1;
							BitSet operand2;
							if(index==arrSQLTerms.length)
								break;
							if(strarrOperators[index-1].equals("XOR") && result[index]==null && !whichFieldHasIndex[index])
							{
								if(result[i]==null)
									result[i]=indicesBitMaps[i];
								int posInArray = orderedColumns.indexOf(arrSQLTerms[index].strColumnName);
        		    			posInArray++;
								for(int j=0;j<result[i].size();j++)
								{
									Object[] myTuple = getTupleFromBitMap(arrSQLTerms[index].strTableName, j);
									if(myTuple==null)
										break;
									if(result[i].get(j))
									{
										if(evaluate(myTuple[posInArray],arrSQLTerms[index].strOperator,arrSQLTerms[index].objValue))
										{
											result[i].flip(j);
										}
									}
									else
									{
										if(evaluate(getTupleFromBitMap(arrSQLTerms[index].strTableName, j)[posInArray],arrSQLTerms[index].strOperator,arrSQLTerms[index].objValue))
										{
											result[i].flip(j);
										}
									}
								}
								bitwiseOpResult=result[i];
								result[index]=result[i];
								flag[i]=true;
	            				flag[index]=true;
	            				entered=true;
							}
							else if(strarrOperators[index-1].equals("XOR") && result[index]==null && whichFieldHasIndex[index])
							{
								if(result[i]==null)
									result[i]=indicesBitMaps[i];
								operand1=result[i];
								operand2=indicesBitMaps[index];
								operand1.xor(operand2);
								bitwiseOpResult=operand1;
								flag[i]=true;
	            				flag[index]=true;
	            				result[i]=operand1;;
	            				result[index]=operand1;	
	            				entered=true;
							}
							else if(strarrOperators[index-1].equals("XOR") && result[index]!=null)
							{
								if(result[i]==null)
									result[i]=indicesBitMaps[i];
								operand1=result[i];
								operand2=result[index];
								operand1.xor(operand2);
								bitwiseOpResult=operand1;
								flag[i]=true;
	            				flag[index]=true;
	            				result[i]=operand1;;
	            				result[index]=operand1;	
	            				entered=true;
							}
							else
							{
								//set all values from index to i to be same result
								if(entered)
								{
									for(int j=i;j<index-1;j++)
									{
										result[j]=result[index-1];
									}
									bitwiseOpResult=result[index-1];
									break;
								}
							}
						}
					}
				}
				// for the rest of the unresolved XORs
				for(int i=0;i<strarrOperators.length;i++)
	        	{
        			if(strarrOperators[i].equals("XOR"))
        			{
        				if(result[i]==null || result[i+1]==null)
        				{
        					BitSet operand1;
            				BitSet operand2;
            				if(result[i]!=null && result[i+1]==null)
            				{
            					operand1=result[i];
            					for (int j = 0;j<operand1.size();j++) 
            					{
            						Object[] myTuple = getTupleFromBitMap(arrSQLTerms[0].strTableName, j);
            						if(myTuple==null)
            							break;
            						int index = orderedColumns.indexOf(arrSQLTerms[i+1].strColumnName);
            						index++;
            						if(operand1.get(j))
            						{
	            						if(evaluate(myTuple[index],arrSQLTerms[i+1].strOperator, arrSQLTerms[i+1].objValue))
	            						{
	            							operand1.flip(j);
	            						}
            						}
	            					else
	            					{
	            						if(evaluate(myTuple[index],arrSQLTerms[i+1].strOperator, arrSQLTerms[i+1].objValue))
	            						{
	            							operand1.flip(j);
	            						}
	            					}
            					}
                				bitwiseOpResult=operand1;
                				flag[i]=true;
                				flag[i+1]=true;
                				result[i]=operand1;;
                				result[i+1]=operand1;
                				continue;
            				}
            				else if(result[i]==null && result[i+1]!=null)
            				{
            					operand2=result[i+1];
            					for (int j = 0;j<operand2.size();j++) 
            					{
            						Object[] myTuple = getTupleFromBitMap(arrSQLTerms[0].strTableName, j);
            						if(myTuple==null)
            							break;
            						int index = orderedColumns.indexOf(arrSQLTerms[i].strColumnName);
            						index++;
            						if(operand2.get(j))
            						{
	            						if(evaluate(myTuple[index],arrSQLTerms[i].strOperator, arrSQLTerms[i].objValue))
	            						{
	            							operand2.flip(j);
	            						}
            						}
	            					else
	            					{
	            						if(evaluate(myTuple[index],arrSQLTerms[i].strOperator, arrSQLTerms[i].objValue))
	            						{
	            							operand2.flip(j);
	            						}
	            					}
            					}
                				bitwiseOpResult=operand2;
                				flag[i]=true;
                				flag[i+1]=true;
                				result[i]=operand2;;
                				result[i+1]=operand2;
                				continue;
            				}
            				else
            				{
            					int index = orderedColumns.indexOf(arrSQLTerms[i].strColumnName);
        		    			index++;
        		    			int index2 = orderedColumns.indexOf(arrSQLTerms[i+1].strColumnName);
        		    			index2++;
        		    			BitSet bitMap=new BitSet();
        		    			int counter=0;
            					for(int j=0;new File("./data/"+arrSQLTerms[i].strTableName+"-"+j).exists();j++)
            					{
            						try
            						{
            							Vector<Object []> page;
            							ObjectInputStream in = new ObjectInputStream(new FileInputStream("./data/"+arrSQLTerms[i].strTableName+"-"+j));
            							Table x = (Table) in.readObject();
            							page = x.table;
            							in.close();
            					        for(int k=0;k<page.size();k++)
            					        {
            					        	if(evaluate(page.get(k)[index], arrSQLTerms[i].strOperator, arrSQLTerms[i].objValue))
            					        	{
            					        		if(!evaluate(page.get(k)[index2], arrSQLTerms[i+1].strOperator, arrSQLTerms[i+1].objValue))
            					        		{
            					        			bitMap.set(counter);
            					        		}
            					        		counter++;
            					        	}
            					        	else
            					        	{
            					        		if(evaluate(page.get(k)[index2], arrSQLTerms[i+1].strOperator, arrSQLTerms[i+1].objValue))
            					        		{
            					        			bitMap.set(counter);
            					        		}
            					        		counter++;
            					        	}
            					        }
            						}
            						catch (Exception e) 
            						{
            					         e.printStackTrace();
            						}
            					}
            					bitwiseOpResult=bitMap;
            					operand1=bitMap;
            					operand2=bitMap;
	        					result[i]=bitMap;
	        					result[i+1]=bitMap;
	        					flag[i]=true;
	        					flag[i+1]=true;
            				}
            				/*
            				if(result[i+1]!=null)
            					operand2=result[i+1];
            				else if(result[i+1]==null && result[i]!=null)
            				{
            					operand1=result[i];
            					for (int j = 0;j<operand1.size();j++) 
            					{
            						Object[] myTuple = getTupleFromBitMap(arrSQLTerms[0].strTableName, j);
            						if(myTuple==null)
            							break;
            						int index = orderedColumns.indexOf(arrSQLTerms[i+1].strColumnName);
            						index++;
            						if(operand1.get(j))
            						{
	            						if(evaluate(myTuple[index],arrSQLTerms[i+1].strOperator, arrSQLTerms[i+1].objValue))
	            						{
	            							operand1.flip(j);
	            						}
            						}
	            					else
	            					{
	            						if(evaluate(myTuple[index],arrSQLTerms[i+1].strOperator, arrSQLTerms[i+1].objValue))
	            						{
	            							operand1.flip(j);
	            						}
	            					}
            					}
                				bitwiseOpResult=operand1;
                				flag[i]=true;
                				flag[i+1]=true;
                				result[i]=operand1;
                				result[i+1]=operand1;
                				continue;
            				}
            				else
            				{
	    						int index = orderedColumns.indexOf(arrSQLTerms[i+1].strColumnName);
	    		    			index++;
	    		    			BitSet bitMap=new BitSet();
	    		    			int counter=0;
	        					for(int j=0;new File("./data/"+arrSQLTerms[i+1].strTableName+"-"+j).exists();j++)
	        					{
	        						try
	        						{
	        							Vector<Object []> page;
	        							ObjectInputStream in = new ObjectInputStream(new FileInputStream("./data/"+arrSQLTerms[i+1].strTableName+"-"+j));
	        							Table x = (Table) in.readObject();
	        							page = x.table;
	        							in.close();
	        					        for(int k=0;k<page.size();k++)
	        					        {
	        					        	if(evaluate(page.get(k)[index], arrSQLTerms[i+1].strOperator, arrSQLTerms[i+1].objValue))
	        					        	{
	        					        		bitMap.set(counter);
	        					        		counter++;
	        					        	}
	        					        	else
            					        		counter++;
	        					        }
	        						}
	        						catch (Exception e) 
	        						{
	        					         e.printStackTrace();
	        						}
	        					}
	        					operand2=bitMap;
	        					operand2=bitMap;
	        					result[i+1]=operand2;
	        					flag[i+1]=true;
	        				}
            				operand1.xor(operand2);
            				bitwiseOpResult=operand1;
            				flag[i]=true;
            				flag[i+1]=true;
            				result[i]=operand1;;
            				result[i+1]=operand1;	
            				*/
        				}
        			}
	        	}
				// OR with no interrupted precedence
				entered=false;
				for(int i=0;i<whichFieldHasIndex.length;i++)
				{
					if(whichFieldHasIndex[i])
					{
						if(i>=strarrOperators.length)
						{
							if(!strarrOperators[i-1].equals("OR"))
								break;
						}
						else
						{
							if(!strarrOperators[i].equals("OR"))
								break;
						}
						//GO LEFT
						int index=i;
						while(index>0)
						{
							index--;
							BitSet operand1;
							BitSet operand2;
							if(strarrOperators[index].equals("OR") && result[index]==null && !whichFieldHasIndex[index])
							{
								if(result[i]==null)
									result[i]=indicesBitMaps[i];
								int posInArray = orderedColumns.indexOf(arrSQLTerms[index].strColumnName);
        		    			posInArray++;
								for(int j=0;j<result[i].size();j++)
								{
									Object[] myTuple = getTupleFromBitMap(arrSQLTerms[index].strTableName, j);
									if(myTuple==null)
										break;
									if(!result[i].get(j))
									{
										if(evaluate(myTuple[posInArray],arrSQLTerms[index].strOperator,arrSQLTerms[index].objValue))
										{
											result[i].flip(j);
										}
									}
								}
								bitwiseOpResult=result[i];
								result[index]=result[i];
								flag[i]=true;
	            				flag[index]=true;
	            				entered=true;
							}
							else if(strarrOperators[index].equals("OR") && result[index]==null && whichFieldHasIndex[index])
							{
								if(result[i]==null)
									result[i]=indicesBitMaps[i];
								operand1=result[i];
								operand2=indicesBitMaps[index];
								operand1.or(operand2);
								bitwiseOpResult=operand1;
								flag[i]=true;
	            				flag[index]=true;
	            				result[i]=operand1;;
	            				result[index]=operand1;	
	            				entered=true;
							}
							else if(strarrOperators[index].equals("OR") && result[index]!=null)
							{
								if(result[i]==null)
									result[i]=indicesBitMaps[i];
								operand1=result[i];
								operand2=result[index];
								operand1.or(operand2);
								bitwiseOpResult=operand1;
								flag[i]=true;
	            				flag[index]=true;
	            				result[i]=operand1;;
	            				result[index]=operand1;	
	            				entered=true;
							}
							else
							{
								if(entered)
								{
									for(int j=index+2;j<=i;j++)
									{
										result[j]=result[index+1];
									}
									bitwiseOpResult=result[index+1];
									break;
								}
							}
						}
						// GO RIGHT
						index=i;
						entered=false;
						while(index<arrSQLTerms.length)
						{
							index++;
							BitSet operand1;
							BitSet operand2;
							if(index==arrSQLTerms.length)
								break;
							if(strarrOperators[index-1].equals("OR") && result[index]==null && !whichFieldHasIndex[index])
							{
								if(result[i]==null)
									result[i]=indicesBitMaps[i];
								int posInArray = orderedColumns.indexOf(arrSQLTerms[index].strColumnName);
        		    			posInArray++;
								for(int j=0;j<result[i].size();j++)
								{
									Object[] myTuple = getTupleFromBitMap(arrSQLTerms[index].strTableName, j);
									if(myTuple==null)
										break;
									if(!result[i].get(j))
									{
										if(evaluate(myTuple[posInArray],arrSQLTerms[index].strOperator,arrSQLTerms[index].objValue))
										{
											result[i].flip(j);
										}
									}
								}
								bitwiseOpResult=result[i];
								result[index]=result[i];
								flag[i]=true;
	            				flag[index]=true;
	            				entered=true;
							}
							else if(strarrOperators[index-1].equals("OR") && result[index]==null && whichFieldHasIndex[index])
							{
								if(result[i]==null)
									result[i]=indicesBitMaps[i];
								operand1=result[i];
								operand2=indicesBitMaps[index];
								operand1.or(operand2);
								bitwiseOpResult=operand1;
								flag[i]=true;
	            				flag[index]=true;
	            				result[i]=operand1;;
	            				result[index]=operand1;	
	            				entered=true;
							}
							else if(strarrOperators[index-1].equals("OR") && result[index]!=null)
							{
								if(result[i]==null)
									result[i]=indicesBitMaps[i];
								operand1=result[i];
								operand2=result[index];
								operand1.or(operand2);
								bitwiseOpResult=operand1;
								flag[i]=true;
	            				flag[index]=true;
	            				result[i]=operand1;;
	            				result[index]=operand1;	
	            				entered=true;
							}
							else
							{
								//set all values from index to i to be same result
								if(entered)
								{
									for(int j=i;j<index-1;j++)
									{
										result[j]=result[index-1];
									}
									bitwiseOpResult=result[index-1];
									break;
								}
							}
						}
					}
				}
				// for the rest of the unresolved ORs
				for(int i=0;i<strarrOperators.length;i++)
	        	{
        			if(strarrOperators[i].equals("OR"))
        			{
        				if(result[i]==null || result[i+1]==null)
        				{
        					BitSet operand1;
            				BitSet operand2;
            				if(result[i]!=null && result[i+1]==null)
            				{
            					operand1=result[i];
            					for (int j = 0;j<operand1.size();j++) 
            					{
            						Object[] myTuple = getTupleFromBitMap(arrSQLTerms[0].strTableName, j);
            						if(myTuple==null)
            							break;
            						int index = orderedColumns.indexOf(arrSQLTerms[i+1].strColumnName);
            						index++;
            						if(!operand1.get(j))
            						{
	            						if(evaluate(myTuple[index],arrSQLTerms[i+1].strOperator, arrSQLTerms[i+1].objValue))
	            						{
	            							operand1.flip(j);
	            						}
            						}
            					}
                				bitwiseOpResult=operand1;
                				flag[i]=true;
                				flag[i+1]=true;
                				result[i]=operand1;;
                				result[i+1]=operand1;
                				continue;
            				}
            				else if(result[i]==null && result[i+1]!=null)
            				{
            					operand2=result[i+1];
            					for (int j = 0;j<operand2.size();j++) 
            					{
            						Object[] myTuple = getTupleFromBitMap(arrSQLTerms[0].strTableName, j);
            						if(myTuple==null)
            							break;
            						int index = orderedColumns.indexOf(arrSQLTerms[i].strColumnName);
            						index++;
            						if(!operand2.get(j))
            						{
	            						if(evaluate(myTuple[index],arrSQLTerms[i].strOperator, arrSQLTerms[i].objValue))
	            						{
	            							operand2.flip(j);
	            						}
            						}
            					 }
                				bitwiseOpResult=operand2;
                				flag[i]=true;
                				flag[i+1]=true;
                				result[i]=operand2;;
                				result[i+1]=operand2;
                				continue;
            				}
            				else
            				{
            					int index = orderedColumns.indexOf(arrSQLTerms[i].strColumnName);
        		    			index++;
        		    			int index2 = orderedColumns.indexOf(arrSQLTerms[i+1].strColumnName);
        		    			BitSet bitMap=new BitSet();
        		    			int counter=0;
            					for(int j=0;new File("./data/"+arrSQLTerms[i].strTableName+"-"+j).exists();j++)
            					{
            						try
            						{
            							Vector<Object []> page;
            							ObjectInputStream in = new ObjectInputStream(new FileInputStream("./data/"+arrSQLTerms[i].strTableName+"-"+j));
            							Table x = (Table) in.readObject();
            							page = x.table;
            							in.close();
            					        for(int k=0;k<page.size();k++)
            					        {
            					        	if(evaluate(page.get(k)[index], arrSQLTerms[i].strOperator, arrSQLTerms[i].objValue))
            					        	{
            					        		bitMap.set(counter);
            					        	}
            					        	else
            					        	{
            					        		if(evaluate(page.get(k)[index2], arrSQLTerms[i+1].strOperator, arrSQLTerms[i+1].objValue))
            					        		{
            					        			bitMap.set(counter);
            					        		}
            					        	}
            					        	counter++;
            					        }
            						}
            						catch (Exception e) 
            						{
            					         e.printStackTrace();
            						}
            					}
            					operand1=bitMap;
            					operand2=bitMap;
	        					result[i]=bitMap;
	        					result[i+1]=bitMap;
	        					flag[i]=true;
	        					flag[i+1]=true;
            				}
            				/*
            				if(result[i+1]!=null)
            					operand2=result[i+1];
            				else if(result[i+1]==null && result[i]!=null)
            				{
            					operand1=result[i];
            					for (int j = 0;j<operand1.size();j++) 
            					{
            						Object[] myTuple = getTupleFromBitMap(arrSQLTerms[0].strTableName, j);
            						if(myTuple==null)
            							break;
            						int index = orderedColumns.indexOf(arrSQLTerms[i+1].strColumnName);
            						index++;
            						if(!operand1.get(j))
            						{
	            						if(evaluate(myTuple[index],arrSQLTerms[i+1].strOperator, arrSQLTerms[i+1].objValue))
	            						{
	            							operand1.flip(j);
	            						}
            						}
            					 }
                				bitwiseOpResult=operand1;
                				flag[i]=true;
                				flag[i+1]=true;
                				result[i]=operand1;;
                				result[i+1]=operand1;
                				continue;
            				}
            				else
            				{
	    						int index = orderedColumns.indexOf(arrSQLTerms[i+1].strColumnName);
	    		    			index++;
	    		    			BitSet bitMap=new BitSet();
	    		    			int counter=0;
	        					for(int j=0;new File("./data/"+arrSQLTerms[i+1].strTableName+"-"+j).exists();j++)
	        					{
	        						try
	        						{
	        							Vector<Object []> page;
	        							ObjectInputStream in = new ObjectInputStream(new FileInputStream("./data/"+arrSQLTerms[i+1].strTableName+"-"+j));
	        							Table x = (Table) in.readObject();
	        							page = x.table;
	        							in.close();
	        					        for(int k=0;k<page.size();k++)
	        					        {
	        					        	if(evaluate(page.get(k)[index], arrSQLTerms[i+1].strOperator, arrSQLTerms[i+1].objValue))
	        					        	{
	        					        		bitMap.set(counter);
	        					        		counter++;
	        					        	}
	        					        	else
            					        		counter++;
	        					        }
	        						}
	        						catch (Exception e) 
	        						{
	        					         e.printStackTrace();
	        						}
	        					}
	        					operand2=bitMap;
	        					operand2=bitMap;
	        					result[i+1]=operand2;
	        					flag[i+1]=true;
	        				}
            				operand1.or(operand2);
            				bitwiseOpResult=operand1;
            				flag[i]=true;
            				flag[i+1]=true;
            				result[i]=operand1;;
            				result[i+1]=operand1;
            				*/	
        				}
        			}
	        	}	
				
				// get rows of the final bitwiseOpResult to put in iterator
				for (int i = bitwiseOpResult.nextSetBit(0); i >= 0; i = bitwiseOpResult.nextSetBit(i+1)) {
				     resultSet.add(getTupleFromBitMap(arrSQLTerms[0].strTableName, i));
				 }
			}	
			// no operator ie: only 1 argument in where and is indexed
			else
			{
				/*
				IndexPage indexPage=null;
				BitSet bitMap=null;
				for(int j=0;new File("./data/"+arrSQLTerms[0].strTableName+"-"+arrSQLTerms[0].strColumnName+"-"+j).exists();j++)
				{
					indexPage = readCompressed(arrSQLTerms[0].strTableName, arrSQLTerms[0].strColumnName, j);
					for(int k=0;k<indexPage.uniqueValues.size();k++)
					{
						if(evaluate(indexPage.uniqueValues.get(k), arrSQLTerms[0].strOperator, arrSQLTerms[0].objValue))
						{
							if(bitMap==null)
								bitMap=convertToBitSet(indexPage.bitMaps.get(k));
							else
							{
								bitMap.or(convertToBitSet(indexPage.bitMaps.get(k)));
							}
						}
					}
				}
				*/
				// put rows in ierator if single argument and indexed
				for (int i = indicesBitMaps[0].nextSetBit(0); i >= 0; i = indicesBitMaps[0].nextSetBit(i+1)) {
				     resultSet.add(getTupleFromBitMap(arrSQLTerms[0].strTableName,i));
				 }
			}
		}
		// not indexed on any column
		else
		{
			for(int i=0;new File("./data/"+arrSQLTerms[0].strTableName+"-"+i).exists();i++)
			{
				try
				{
					Vector<Object []> page;
					ObjectInputStream in = new ObjectInputStream(new FileInputStream("./data/"+arrSQLTerms[0].strTableName+"-"+i));
			        Table x = (Table) in.readObject();
					page = x.table;
			        in.close();
			        for(int j=0;j<page.size();j++)
			        {
			        	boolean bitwiseOpResult=false;
			        	if(strarrOperators.length>0)
			        	{
			        		boolean[]flag = new boolean[arrSQLTerms.length];
			        		boolean[]result= new boolean[arrSQLTerms.length];
			        		boolean[]changed = new boolean[arrSQLTerms.length];
			        		for(int k=0;k<strarrOperators.length;k++)
				        	{
			        			if(strarrOperators[k].equals("AND"))
			        			{
			        				boolean operand1;
			        				boolean operand2;
			        				if(flag[k]==true)
			        					operand1=result[k];
			        				else
			        				{
			        					int index = orderedColumns.indexOf(arrSQLTerms[k].strColumnName);
			    		    			index++;
			        					operand1=evaluate(page.get(j)[index], arrSQLTerms[k].strOperator, arrSQLTerms[k].objValue);
			        				}
			        				if(flag[k+1]==true)
			        					operand2=result[k+1];
			        				else
			        				{
			        					int index = orderedColumns.indexOf(arrSQLTerms[k+1].strColumnName);
			    		    			index++;
			        					operand2=evaluate(page.get(j)[index], arrSQLTerms[k+1].strOperator, arrSQLTerms[k+1].objValue);
			        				}
			        				bitwiseOpResult=(operand1 && operand2);
			        				flag[k]=true;
			        				flag[k+1]=true;
			        				result[k]=bitwiseOpResult;
			        				result[k+1]=bitwiseOpResult;
			        				changed[k]=true;
			        				changed[k+1]=true;
			        			}
			        			else
			        			{
			        				for(int l=0;l<changed.length;l++)
			        				{
			        					if(changed[l])
			        					{
			        						result[l]=bitwiseOpResult;
			        					}
			        				}
			        				changed=new boolean[arrSQLTerms.length];
			        			}
				        	}
			        		for(int l=0;l<changed.length;l++)
	        				{
	        					if(changed[l])
	        					{
	        						result[l]=bitwiseOpResult;
	        					}
	        				}
			        		changed=new boolean[arrSQLTerms.length];
			        		for(int k=0;k<strarrOperators.length;k++)
				        	{
			        			if(strarrOperators[k].equals("XOR"))
			        			{
			        				boolean operand1;
			        				boolean operand2;
			        				if(flag[k]==true)
			        					operand1=result[k];
			        				else
			        				{
			        					int index = orderedColumns.indexOf(arrSQLTerms[k].strColumnName);
			    		    			index++;
			        					operand1=evaluate(page.get(j)[index], arrSQLTerms[k].strOperator, arrSQLTerms[k].objValue);
			        				}
			        				if(flag[k+1]==true)
			        					operand2=result[k+1];
			        				else
			        				{
			        					int index = orderedColumns.indexOf(arrSQLTerms[k+1].strColumnName);
			    		    			index++;
			        					operand2=evaluate(page.get(j)[index], arrSQLTerms[k+1].strOperator, arrSQLTerms[k+1].objValue);
			        				}
			        				bitwiseOpResult=(operand1 ^ operand2);
			        				flag[k]=true;
			        				flag[k+1]=true;
			        				result[k]=bitwiseOpResult;
			        				result[k+1]=bitwiseOpResult;
			        				changed[k]=true;
			        				changed[k+1]=true;
			        			}
			        			else
			        			{
			        				for(int l=0;l<changed.length;l++)
			        				{
			        					if(changed[l])
			        					{
			        						result[l]=bitwiseOpResult;
			        					}
			        				}
			        				changed=new boolean[arrSQLTerms.length];
			        			}
				        	}
			        		for(int l=0;l<changed.length;l++)
	        				{
	        					if(changed[l])
	        					{
	        						result[l]=bitwiseOpResult;
	        					}
	        				}
			        		changed=new boolean[arrSQLTerms.length];
			        		for(int k=0;k<strarrOperators.length;k++)
				        	{
			        			if(strarrOperators[k].equals("OR"))
			        			{
			        				boolean operand1;
			        				boolean operand2;
			        				if(flag[k]==true)
			        					operand1=result[k];
			        				else
			        				{
			        					int index = orderedColumns.indexOf(arrSQLTerms[k].strColumnName);
			    		    			index++;
			        					operand1=evaluate(page.get(j)[index], arrSQLTerms[k].strOperator, arrSQLTerms[k].objValue);
			        				}
			        				if(flag[k+1]==true)
			        					operand2=result[k+1];
			        				else
			        				{
			        					int index = orderedColumns.indexOf(arrSQLTerms[k+1].strColumnName);
			    		    			index++;
			        					operand2=evaluate(page.get(j)[index], arrSQLTerms[k+1].strOperator, arrSQLTerms[k+1].objValue);
			        				}
			        				bitwiseOpResult=(operand1 || operand2);
			        				flag[k]=true;
			        				flag[k+1]=true;
			        				result[k]=bitwiseOpResult;
			        				result[k+1]=bitwiseOpResult;
			        				changed[k]=true;
			        				changed[k+1]=true;
			        			}
			        			else
			        			{
			        				for(int l=0;l<changed.length;l++)
			        				{
			        					if(changed[l])
			        					{
			        						result[l]=bitwiseOpResult;
			        					}
			        				}
			        				changed=new boolean[arrSQLTerms.length];
			        			}
				        	}
			        		if(bitwiseOpResult)
			        		{
			        			resultSet.add(page.get(j));
			        		}
			        	}
			        	// only 1 argument
			        	else
			        	{
			        		int index = orderedColumns.indexOf(arrSQLTerms[0].strColumnName);
    		    			index++;
        					if(evaluate(page.get(j)[index], arrSQLTerms[0].strOperator, arrSQLTerms[0].objValue))
        					{
        						resultSet.add(page.get(j));
        					}
			        	}
			        }
				}
				catch (Exception e) 
				{
			         e.printStackTrace();
				}
			}
		}
		it=resultSet.iterator();
		return it;	
	}

	//General Helpers
	private static boolean compareTypes(String colType, Object passed)
	{
		switch (colType)
		{
		case "java.lang.Integer":
			if(passed instanceof Integer)
				return true;
			else
				return false;
		case "java.lang.String":
			if(passed instanceof String)
				return true;
			else
				return false;
		case "java.lang.Double":
			if(passed instanceof Double)
				return true;
			else
				return false;
		case "java.lang.Boolean":
			if(passed instanceof Boolean)
				return true;
			else
				return false;
		case "java.util.Date":
			if(passed instanceof Date)
				return true;
			else
				return false;
		default:
			return false;
		}
	}
	private static void displayIndex(String strTableName, String strColName)
	{
		for(int i=0;new File("./data/"+strTableName+"-"+strColName+"-"+i).exists();i++)
			System.out.println(readCompressed(strTableName, strColName, i));
	}
	private static void displayTable(String strTableName)
	{
		for(int i=0;new File("./data/"+strTableName+"-"+i).exists();i++)
		{
			Table myTable=readTable(strTableName, i);
			for(int j=0;j<myTable.table.size();j++)
				System.out.println(Arrays.toString(myTable.table.get(j)));
			System.out.println("------------------");
		}
		System.out.println("=====================================");
	}
	private static int compare(Object a,Object b) throws DBAppException
	{
		if(a instanceof Integer && b instanceof Integer)
		{
			int first=(int) a;
			int sec=(int) b;
			return first-sec;
		}
		else if(a instanceof String && b instanceof String)
		{
			String first=(String) a;
			String sec=(String) b;
			return first.compareTo(sec);
		}
		else if(a instanceof Double && b instanceof Double)
		{
			double first = (Double) a;
			double sec = (Double) b;
				return (int)Math.ceil(first-sec);
		}
		else if(a instanceof Boolean && b instanceof Boolean)
		{
			boolean first=(boolean) a;
			boolean sec=(boolean) b;
			return first==sec? 0:1;
		}
		else if(a instanceof Date && b instanceof Date)
		{
			try
			{
				Date first=(Date) a;
				Date second=(Date) b;
				return first.compareTo(second);
			}
			catch(Exception e){return 1;}
		}
		else if(a==null && (b instanceof Integer || b instanceof Double || b instanceof String || b instanceof Boolean || b instanceof Date))
			return -1;
		else if(b==null && (a instanceof Integer || a instanceof Double || a instanceof String || a instanceof Boolean || a instanceof Date))
			return 1;
		else if(a==null && b==null)
			return 0;
		throw new DBAppException("Invalid data type");
	}
	private static ArrayList<String []> getRelatedMetaData(String tableName)
	{
		ArrayList<String []> relatedMetaData=new ArrayList<String []>();
		try 
		{
			FileReader reader=new FileReader("./data/metadata.csv");
			BufferedReader bf=new BufferedReader(reader);
			String [] touchDate= {tableName,"TouchDate","java.util.Date","false","false"};
			relatedMetaData.add(touchDate);
			bf.readLine();
			while(bf.ready())
			{
				String [] myArr=new String[5];
				StringTokenizer stringTokenizer=new StringTokenizer(bf.readLine(),",");
				String name=stringTokenizer.nextToken();
				if(name.equals(tableName))
				{	myArr[0]=name;
					for(int i=1;stringTokenizer.hasMoreTokens();i++)
						myArr[i]=stringTokenizer.nextToken();
					relatedMetaData.add(myArr);
				}
			}
			bf.close();
			if(relatedMetaData.isEmpty())
				throw new DBAppException("This table doesn't exist.");
		} 
		catch (Exception e) {e.printStackTrace();}
		return relatedMetaData;
	}
	private static Object [] toObjectArray(String tableName, Hashtable<String,Object> htblColNameValue) throws DBAppException
	{
		ArrayList<String []> relatedMetaData=getRelatedMetaData(tableName);
		Object [] toBeInserted=new Object [relatedMetaData.size()];
		for(int i=0;i<relatedMetaData.size();i++)
		{
			String colName=null;
			Object data=null;
			for(String key: htblColNameValue.keySet())
			{
				if(key.equals(relatedMetaData.get(i)[1]))
				{
					colName=key;
					data=htblColNameValue.get(key);
					boolean f=relatedMetaData.get(i)[2].equals(data.getClass().getName());
					if(!f)
						{
							System.out.println(relatedMetaData.get(i)[2]);
							System.out.println(data.getClass().getName());
							throw new DBAppException("Entry data type doesn't match column's data type");
						}
						
					if(colName==null || data==null)
						throw new DBAppException("Cannot enter null values");
					break;
				}
			}
			toBeInserted[i]=data;
		}
		return toBeInserted;
	}
	private static int[] findPlace(String strTableName,Object [] record) throws DBAppException
	{
		/*
		 * This method returns an array of integers where 
		 * location zero holds the pageNo and location 1 holds placeInPage
		 */
		int [] ans=new int [2];
		ArrayList<String []> relatedMetaData=getRelatedMetaData(strTableName);
		int indexOfClusteringColumn=0;
		String strColName="";
		boolean isIndexed=false;
		for(int i=0;i<relatedMetaData.size();i++)
		{
			if(relatedMetaData.get(i)[3].equals("true"))
			{
				indexOfClusteringColumn=i;
				strColName=relatedMetaData.get(i)[1];
				isIndexed=Boolean.parseBoolean(relatedMetaData.get(i)[4]);
				break;
			}
		}
		if(isIndexed)
		{
			final String clusterkeyname = strColName;
			File dir = new File("./data/");
			File[] foundFiles = dir.listFiles(new FilenameFilter() {
			    public boolean accept(File dir, String name) {
			        return name.startsWith(strTableName+"-"+clusterkeyname+"-"); //&& name.indexOf("-",strTableName.length()+1)!=-1;
			    }
			});
			int [] x=  bsInsertUpdate(record[indexOfClusteringColumn], strTableName, strColName, foundFiles.length-1);
			Table myTable=readTable(strTableName, x[0]);
			if(myTable!=null&&myTable.table.size()>=x[1]+1 &&compare(myTable.table.get(x[1])[indexOfClusteringColumn], record[indexOfClusteringColumn])==0)
			{
				throw new DBAppException("Clustering key must be unique");
			}
			else
			{
				return x;
			}
//			int i;
//			for(i=0;new File("./data/"+strTableName+"-"+strColName+"-"+i).exists();i++)
//			{
//				IndexPage indexPage=readCompressed(strTableName, strColName, i);
//				for(int j=0;j<indexPage.uniqueValues.size();j++)
//				{
//					Object recordClusteringColumn=record[indexOfClusteringColumn];
//					Object tableClusteringColumn=indexPage.uniqueValues.get(j);
//					if(compare(recordClusteringColumn, tableClusteringColumn)<0)
//					{
//						int tupleNo=0;
//						for(int k=0;k<indexPage.bitMaps.get(j).size();k++)
//							if(indexPage.bitMaps.get(j).get(k)==1)
//							{
//								tupleNo=k;
//								break;
//							}
//						int posInPage= tupleNo%maxSizeOfPage;
//						int pageNumber= (int) Math.floor(tupleNo/maxSizeOfPage);
//						ans[0]=pageNumber; ans[1]=posInPage;
//						return ans;
//					}
//					else if(compare(recordClusteringColumn, tableClusteringColumn)==0)
//					{
//						throw new DBAppException("Clustering key must be unique");
//					}
//				}
//			}
//			//since this record should be inserted last
//			IndexPage indexPage=readCompressed(strTableName, strColName, Math.max(0, i-1));
//			int numberOfTuplesInTable=0;
//			if(indexPage!=null && indexPage.bitMaps.size()!=0)
//				numberOfTuplesInTable=indexPage.bitMaps.get(0).size();
//			
//			int pageNumber= (int) Math.floor(numberOfTuplesInTable/maxSizeOfPage);
//			int posInPage= numberOfTuplesInTable%maxSizeOfPage;
//			
//			ans[0]=pageNumber; ans[1]=posInPage;
//			return ans;
		}
		else
		{
			int i=0;
			int j=0;
			for(i=0;new File("./data/"+strTableName+"-"+i).exists();i++)
			{
				Table myTable=readTable(strTableName, i);
				
				for(j=0;j<myTable.table.size();j++)
				{
					Object recordClusteringColumn=record[indexOfClusteringColumn];
					Object tableClusteringColumn=myTable.table.get(j)[indexOfClusteringColumn];
					if(compare(recordClusteringColumn, tableClusteringColumn)<0)
					{
						ans[0]=i; ans[1]=j;
						return ans;
					}
					else if(compare(recordClusteringColumn, tableClusteringColumn)==0)
					{
						throw new DBAppException("Clustering key must be unique");
					}
				}
				if(j!=maxSizeOfPage)
				{
					ans[0]=i; ans[1]=j;
					return ans;
				}
			}
			ans[0]=i;
			ans[1]=0;
			
			return ans;
		}
	}
	
	private static int[] bsInsertUpdate (Object toBeInserted, String strTableName, String strColumnName, int lastPageIndex) throws DBAppException
	{
		IndexPage indexPage=null;
		ArrayList<Object> uniqueValues;
		int posInPage = -1;
		int pageNumber = -1;
		int elemNumber = 0;
		int[] position = new int[3];
		
		if(toBeInserted instanceof Boolean)
		{
			throw new DBAppException("Boolean not supported in Binary Search");
		}
		
	    int leftT = 0;
		int rightT = lastPageIndex+1;
		int mT =0;
		int leftP=0;
		int rightP=0;
	    while (leftT<rightT)
	    {
	        mT = (leftT+rightT)/2;
	        indexPage = readCompressed(strTableName, strColumnName, mT);
	        uniqueValues = indexPage.uniqueValues;
	        leftP = 0;
	        rightP = uniqueValues.size();
	        while(leftP<rightP)
	        {
	        	int mP = (leftP+rightP)/2;
	        	if(evaluate(uniqueValues.get(mP), ">=",toBeInserted))
	        	{
	        		rightP= mP;
	        	}
	        	else
	        	{
	        		leftP=mP+1;
	        	}
	        }
	        if(leftP == 0 && evaluate(uniqueValues.get(0), "!=", toBeInserted))
	        {
	        	rightT = mT;
	        }
	        else if(leftP == uniqueValues.size())
	        {
	        	leftT = mT+1;
	        }
	        else if (leftP == 0 && evaluate(uniqueValues.get(0), "=", toBeInserted))
	        {
	        	for(int i=0;i<=mT;i++)
	        	{
	        		ArrayList<Object> kk = readCompressed(strTableName, strColumnName, i).uniqueValues;
	        		for(int j=0;j<kk.size();j++)
	        		{
	        			if(evaluate(kk.get(j), "<=", toBeInserted))
	        			{
	        				elemNumber++;
	        			}
	        		}
	        	}
	        	elemNumber--;
	        	//elemNumber = mT*maxSizeOfIndex + leftP;
	        	posInPage= elemNumber%maxSizeOfPage;
	        	pageNumber= (int) Math.floor(elemNumber/maxSizeOfPage);
	        	position[0]=pageNumber;
	        	position[1]=posInPage;
	        	position[2]=mT;
	        	return position;
	        }
	        else if(evaluate(uniqueValues.get(leftP),"=", toBeInserted))
	        {
	        	for(int i=0;i<=mT;i++)
	        	{
	        		ArrayList<Object> kk = readCompressed(strTableName, strColumnName, i).uniqueValues;
	        		for(int j=0;j<kk.size();j++)
	        		{
	        			if(evaluate(kk.get(j), "<=", toBeInserted))
	        			{
	        				elemNumber++;
	        			}
	        		}
	        	}
	        	elemNumber--;
	        	//elemNumber = mT*maxSizeOfIndex + leftP;
	        	posInPage= elemNumber%maxSizeOfPage;
	        	pageNumber= (int) Math.floor(elemNumber/maxSizeOfPage);
	        	position[0]=pageNumber;
	        	position[1]=posInPage;
	        	position[2]=mT;
	        	return position;
	        }
	        else
	        {
	        	for(int i=0;i<=mT;i++)
	        	{
	        		ArrayList<Object> kk = readCompressed(strTableName, strColumnName, i).uniqueValues;
	        		for(int j=0;j<kk.size();j++)
	        		{
	        			if(evaluate(kk.get(j), "<=", toBeInserted))
	        			{
	        				elemNumber++;
	        			}
	        		}
	        	}
	        	elemNumber--;
	        	//elemNumber = mT*maxSizeOfIndex + leftP;
	        	posInPage= elemNumber%maxSizeOfPage;
	        	pageNumber= (int) Math.floor(elemNumber/maxSizeOfPage);
	        	position[0]=pageNumber;
	        	position[1]=posInPage;
	        	position[2]=mT;
	        	return position;
	        }
	    }
	    for(int i=0;i<=mT;i++)
    	{
    		ArrayList<Object> kk = readCompressed(strTableName, strColumnName, i).uniqueValues;
    		for(int j=0;j<kk.size();j++)
    		{
    			if(evaluate(kk.get(j), "<=", toBeInserted))
    			{
    				elemNumber++;
    			}
    		}
    	}
    	//elemNumber--;
	    //elemNumber = mT*maxSizeOfIndex + leftP;
    	posInPage= elemNumber%maxSizeOfPage;
    	pageNumber= (int) Math.floor(elemNumber/maxSizeOfPage);
    	position[0]=pageNumber;
    	position[1]=posInPage;
    	position[2]=mT;
		return position;
	}
	
	private static Table readTable(String strTableName,int i)
	{
		Table myTable=null;
		try
		{
			File f = new File("./data/"+strTableName+"-"+i);
			if(!f.exists())
				return null;
			FileInputStream fis = new FileInputStream(f);
			ObjectInputStream ois = new ObjectInputStream(fis);
			myTable=(Table) ois.readObject();
			ois.close();
		}catch (Exception e) {e.printStackTrace(); }
		return myTable;
	}
	private static void writeTable(String strTableName,int i,Table myTable)
	{
		try
		{
			FileOutputStream fos=new FileOutputStream(new File("./data/"+strTableName+"-"+i));
			ObjectOutputStream oos=new ObjectOutputStream(fos);
			oos.writeObject(myTable);
			oos.close();
		} 
		catch (IOException e) {e.printStackTrace();}
	}
	
	// SELECT helpers
	
	//Select Helpers
	private static boolean evaluate(Object op1, String operator, Object op2) throws DBAppException
	{
		if(op1==null && op2==null && operator.equals("="))
			return true;
		if(op1 ==  null || op2==null)
			return false;
		switch (operator) 
		{
		case ">":
		{
			if(op1 instanceof String)
			{
				String first=(String)op1;
				String second=(String)op2;
				if(first.compareTo(second)>0)
					return true;
				else
					return false;
			}
			if(op1 instanceof Boolean)
				throw new DBAppException("Invalid Operator "+operator);
			if(op1 instanceof Integer)
			{
				int first=(int)op1;
				int second;
				if(op2 instanceof String)
					second=Integer.parseInt((String) op2);
				else
					second=(int) op2;
				if(first>second)
					return true;
				else
					return false;
			}
			if(op1 instanceof Double)
			{
				double first=(double)op1;
				double second;
				if(op2 instanceof String)
					second=Double.parseDouble((String) op2);
				else
					second=(double) op2;
				if(first>second)
					return true;
				else
					return false;
			}
			if(op1 instanceof Date)
			{
				Date first=(Date) op1;
				Date second = null;
				SimpleDateFormat formatter=new SimpleDateFormat("DD/MM/YYYY");
				if(op2 instanceof String)
				{
					try {
						second=formatter.parse((String) op2);
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}
				else
					second=(Date)op2;
				if(first.compareTo(second)>0)
					return true;
				else
					return false;
			}
		}
		case ">=":
		{
			if(op1 instanceof String)
			{
				String first=(String)op1;
				String second=(String)op2;
				if(first.compareTo(second)>=0)
					return true;
				else
					return false;
			}
			if(op1 instanceof Boolean)
				throw new DBAppException("Invalid Operator "+operator);
			if(op1 instanceof Integer)
			{
				int first=(int)op1;
				int second;
				if(op2 instanceof String)
					second=Integer.parseInt((String) op2);
				else
					second=(int) op2;
				if(first>=second)
					return true;
				else
					return false;
			}
			if(op1 instanceof Double)
			{
				double first=(double)op1;
				double second;
				if(op2 instanceof String)
					second=Double.parseDouble((String) op2);
				else
					second=(double) op2;
				if(first>=second)
					return true;
				else
					return false;
			}
			if(op1 instanceof Date)
			{
				Date first=(Date) op1;
				Date second = null;
				SimpleDateFormat formatter=new SimpleDateFormat("DD/MM/YYYY");
				if(op2 instanceof String)
				{
					try {
						second=formatter.parse((String) op2);
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}
				else
					second=(Date)op2;
				if(first.compareTo(second)>=0)
					return true;
				else
					return false;
			}
		}
		case "<":
		{
			if(op1 instanceof String)
			{
				String first=(String)op1;
				String second=(String)op2;
				if(first.compareTo(second)<0)
					return true;
				else
					return false;
			}
			if(op1 instanceof Boolean)
				throw new DBAppException("Invalid Operator "+operator);
			if(op1 instanceof Integer)
			{
				int first=(int)op1;
				int second;
				if(op2 instanceof String)
					second=Integer.parseInt((String) op2);
				else
					second=(int) op2;
				if(first<second)
					return true;
				else
					return false;
			}
			if(op1 instanceof Double)
			{
				double first=(double)op1;
				double second;
				if(op2 instanceof String)
					second=Double.parseDouble((String) op2);
				else
					second=(double) op2;
				if(first<second)
					return true;
				else
					return false;
			}
			if(op1 instanceof Date)
			{
				Date first=(Date) op1;
				Date second = null;
				SimpleDateFormat formatter=new SimpleDateFormat("DD/MM/YYYY");
				if(op2 instanceof String)
				{
					try {
						second=formatter.parse((String) op2);
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}
				else
					second=(Date)op2;
				if(first.compareTo(second)<0)
					return true;
				else
					return false;
			}
		}
		case "<=":
		{
			if(op1 instanceof String)
			{
				String first=(String)op1;
				String second=(String)op2;
				if(first.compareTo(second)<=0)
					return true;
				else
					return false;
			}
			if(op1 instanceof Boolean)
				throw new DBAppException("Invalid Operator "+operator);
			if(op1 instanceof Integer)
			{
				int first=(int)op1;
				int second;
				if(op2 instanceof String)
					second=Integer.parseInt((String) op2);
				else
					second=(int) op2;
				if(first<=second)
					return true;
				else
					return false;
			}
			if(op1 instanceof Double)
			{
				double first=(double)op1;
				double second;
				if(op2 instanceof String)
					second=Double.parseDouble((String) op2);
				else
					second=(double) op2;
				if(first<=second)
					return true;
				else
					return false;
			}
			if(op1 instanceof Date)
			{
				Date first=(Date) op1;
				Date second = null;
				SimpleDateFormat formatter=new SimpleDateFormat("DD/MM/YYYY");
				if(op2 instanceof String)
				{
					try {
						second=formatter.parse((String) op2);
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}
				else
					second=(Date)op2;
				if(first.compareTo(second)<=0)
					return true;
				else
					return false;
			}
		}
		case "!=":
		{
			if(op1 instanceof String)
			{
				String first=(String)op1;
				String second=(String)op2;
				if(first.equals(second))
					return false;
				else
					return true;
			}
			if(op1 instanceof Boolean)
			{
				boolean first=(boolean)op1;
				boolean second;
				if(op2 instanceof String)
					second=Boolean.parseBoolean((String) op2);
				else
					second=(boolean) op2;
				if(first!=second)
					return true;
				else
					return false;
			}
			if(op1 instanceof Integer)
			{
				int first=(int)op1;
				int second;
				if(op2 instanceof String)
					second=Integer.parseInt((String) op2);
				else
					second=(int) op2;
				if(first!=second)
					return true;
				else
					return false;
			}
			if(op1 instanceof Double)
			{
				double first=(double)op1;
				double second;
				if(op2 instanceof String)
					second=Double.parseDouble((String) op2);
				else
					second=(double) op2;
				if(first!=second)
					return true;
				else
					return false;
			}
			if(op1 instanceof Date)
			{
				Date first=(Date) op1;
				Date second = null;
				SimpleDateFormat formatter=new SimpleDateFormat("DD/MM/YYYY");
				if(op2 instanceof String)
				{
					try {
						second=formatter.parse((String) op2);
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}
				else
					second=(Date)op2;
				if(first.compareTo(second)!=0)
					return true;
				else
					return false;
			}
		}
		case "=":
		{
			if(op1 instanceof String)
			{
				String first=(String)op1;
				String second=(String)op2;
				if(first.equals(second))
					return true;
				else
					return false;
			}
			if(op1 instanceof Boolean)
			{
				boolean first=(boolean)op1;
				boolean second;
				if(op2 instanceof String)
					second=Boolean.parseBoolean((String) op2);
				else
					second=(boolean) op2;
				if(first==second)
					return true;
				else
					return false;
			}
			if(op1 instanceof Integer)
			{
				int first=(int)op1;
				int second;
				if(op2 instanceof String)
					second=Integer.parseInt((String) op2);
				else
					second=(int) op2;
				if(first==second)
					return true;
				else
					return false;
			}
			if(op1 instanceof Double)
			{
				double first=(double)op1;
				double second;
				if(op2 instanceof String)
					second=Double.parseDouble((String) op2);
				else
					second=(double) op2;
				if(first==second)
					return true;
				else
					return false;
			}
			if(op1 instanceof Date)
			{
				Date first=(Date) op1;
				Date second = null;
				SimpleDateFormat formatter=new SimpleDateFormat("DD/MM/YYYY");
				if(op2 instanceof String)
				{
					try {
						second=formatter.parse((String) op2);
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}
				else
					second=(Date)op2;
				if(first.compareTo(second)==0)
					return true;
				else
					return false;
			}
		}
		default: throw new DBAppException("Invalid Operator "+operator);
		}
	}
	private static BitSet convertToBitSet(ArrayList<Integer> input)
	{
		BitSet output=new BitSet();
		for(int i=0;i<input.size();i++)
		{
			if(input.get(i)==1)
				output.set(i);
			else
				output.clear(i);
		}
		return output;
	}
	private static Object[] getTupleFromBitMap(String tableName, int tupleNo)
	{
		int posInPage= tupleNo%maxSizeOfPage;
		int pageNumber= (int) Math.floor(tupleNo/maxSizeOfPage);
		Object[] result=null;
		if(!(new File("./data/"+tableName+"-"+pageNumber).exists()))
			return null;
		try
		{
			Vector<Object []> page;
			ObjectInputStream in = new ObjectInputStream(new FileInputStream("./data/"+tableName+"-"+pageNumber));
			Table x = (Table) in.readObject();
			page = x.table;
			in.close();
			if(posInPage>=page.size())
				return null;
			result=page.get(posInPage);
		}
		catch (Exception e) 
		{
	         e.printStackTrace();
		}
		return result;
	}
	@SuppressWarnings("unused")
	private static Object[] getTupleFromBitMapUncontiguous(String tableName, int tupleNo)
	{
		int count=0;
		Object[] result=null;
		for(int i=0;new File("./data/"+tableName+"-"+i).exists();i++)
		{
			try
			{
				Vector<Object []> page;
				ObjectInputStream in = new ObjectInputStream(new FileInputStream("./data/"+tableName+"-"+i));
				Table x = (Table) in.readObject();
				page = x.table;
				in.close();
		        for(int j=0;j<page.size();j++)
		        {
		        	if(page.get(j)!=null)
		        	{
		        		if(count==tupleNo)
		        		{
		        			result=page.get(j);
		        		}
		        		count++;
		        	}
		        }
			}
			catch (Exception e) 
			{
		         e.printStackTrace();
			}
		}
		return result;
	}
	private static BitSet bs (SQLTerm arrSQLTerms, int lastPageIndex) throws DBAppException
	{
		IndexPage indexPage=null;
		ArrayList<Object> uniqueValues;
		
	    int leftT = 0;
		int rightT = lastPageIndex+1;
		int mT=0;
		int leftP=-1;
		int rightP=-1;
	    while (leftT<rightT)
	    {
	        mT = (leftT+rightT)/2;
	        indexPage = readCompressed(arrSQLTerms.strTableName, arrSQLTerms.strColumnName, mT);
	        uniqueValues = indexPage.uniqueValues;
	        leftP = 0;
	        rightP = uniqueValues.size();
	        while(leftP<rightP)
	        {
	        	int mP = (leftP+rightP)/2;
	        	if(evaluate(uniqueValues.get(mP), ">=", arrSQLTerms.objValue))
	        	{
	        		rightP= mP;
	        	}
	        	else
	        	{
	        		leftP=mP+1;
	        	}
	        }
	        if(leftP == 0 && evaluate(uniqueValues.get(0), "!=", arrSQLTerms.objValue))
	        {
	        	rightT = mT;
	        }
	        else if(leftP == uniqueValues.size())
	        {
	        	leftT = mT+1;
	        }
	        else if (leftP == 0 && evaluate(uniqueValues.get(0), "=", arrSQLTerms.objValue))
	        {
	        	switch (arrSQLTerms.strOperator)
	        	{
	        	case ">": 
	        	{
	        		if(leftP == uniqueValues.size()-1)
	        			return getBits(mT+1, 0, arrSQLTerms, true);
	        		else
	        			return getBits(mT, leftP+1, arrSQLTerms, true);
	        	}
	        	case ">=": return getBits(mT, leftP, arrSQLTerms, true);
	        	case "<": return getBits(mT-1, maxSizeOfPage-1, arrSQLTerms, false);
	        	case "<=": return getBits(mT, leftP, arrSQLTerms, false);
	        	case "=": return convertToBitSet(indexPage.bitMaps.get(leftP));
	        	case "!=": throw new DBAppException("!= is bad with Binary search");
	        	default: throw new DBAppException("Incorrect operator "+arrSQLTerms.strOperator);
	        	}
	        }
	        else if(evaluate(uniqueValues.get(leftP),"=", arrSQLTerms.objValue))
	        {
	        	switch (arrSQLTerms.strOperator)
	        	{
	        	case ">": 
	        	{
	        		if(leftP == uniqueValues.size()-1)
	        			return getBits(mT+1, 0, arrSQLTerms, true);
	        		else
	        			return getBits(mT, leftP+1, arrSQLTerms, true);
	        	}
	        	case ">=": return getBits(mT, leftP, arrSQLTerms, true);
	        	case "<": return getBits(mT, leftP-1, arrSQLTerms, false);
	        	case "<=": return getBits(mT, leftP, arrSQLTerms, false);
	        	case "=": return convertToBitSet(indexPage.bitMaps.get(leftP));
	        	case "!=": throw new DBAppException("!= is bad with Binary search");
	        	default: throw new DBAppException("Incorrect operator "+arrSQLTerms.strOperator);
	        	}
	        }
	        else
	        {
	        	switch (arrSQLTerms.strOperator)
	        	{
	        	case ">": return getBits(mT, leftP, arrSQLTerms, true);
	        	case ">=": return getBits(mT, leftP, arrSQLTerms, true);
	        	case "<": return getBits(mT, leftP-1, arrSQLTerms, false);
	        	case "<=": return getBits(mT, leftP-1, arrSQLTerms, false);
	        	case "=": return new BitSet();
	        	case "!=": throw new DBAppException("!= is bad with Binary search");
	        	default: throw new DBAppException("Incorrect operator "+arrSQLTerms.strOperator);
	        	}
	        }
	    }
	    switch (arrSQLTerms.strOperator)
    	{
    	case ">": return getBits(mT, 0, arrSQLTerms, true);
    	case ">=": return getBits(mT, 0, arrSQLTerms, true);
    	case "<": return getBits(mT, readCompressed(arrSQLTerms.strTableName, arrSQLTerms.strColumnName, mT).uniqueValues.size()-1, arrSQLTerms, false);
    	case "<=": return getBits(mT, readCompressed(arrSQLTerms.strTableName, arrSQLTerms.strColumnName, mT).uniqueValues.size()-1, arrSQLTerms, false);
    	case "=": return new BitSet();
    	case "!=": throw new DBAppException("!= is bad with Binary search");
    	default: throw new DBAppException("Incorrect operator "+arrSQLTerms.strOperator);
    	}
	}
	private static BitSet getBits (int startPage, int startTuple, SQLTerm arrSQLTerms, boolean forward) throws DBAppException
	{
		IndexPage indexPage=null;
		BitSet bitMap=null;
		boolean flag=false;
		if(forward)
		{
			for(int i=startPage;new File("./data/"+arrSQLTerms.strTableName+"-"+arrSQLTerms.strColumnName+"-"+i).exists();i++)
			{
				indexPage = readCompressed(arrSQLTerms.strTableName, arrSQLTerms.strColumnName, i);
				for(int j= (flag)?0:startTuple;j<indexPage.uniqueValues.size();j++)
				{
					if(evaluate(indexPage.uniqueValues.get(j), arrSQLTerms.strOperator, arrSQLTerms.objValue))
					{
						if(bitMap==null)
						{
							bitMap=convertToBitSet(indexPage.bitMaps.get(j));
						}
						else
						{
							bitMap.or(convertToBitSet(indexPage.bitMaps.get(j)));
						}
					}
				}
				flag=true;
			}
			if(bitMap==null)
			{
				bitMap=new BitSet();
			}
		}
		else
		{
			flag=false;
			for(int i=startPage;new File("./data/"+arrSQLTerms.strTableName+"-"+arrSQLTerms.strColumnName+"-"+i).exists();i--)
			{
				indexPage = readCompressed(arrSQLTerms.strTableName, arrSQLTerms.strColumnName, i);
				for(int j=(flag)?indexPage.uniqueValues.size()-1:startTuple;j>=0;j--)
				{
					if(evaluate(indexPage.uniqueValues.get(j), arrSQLTerms.strOperator, arrSQLTerms.objValue))
					{
						if(bitMap==null)
						{
							bitMap=convertToBitSet(indexPage.bitMaps.get(j));
						}
						else
						{
							bitMap.or(convertToBitSet(indexPage.bitMaps.get(j)));
						}
					}
				}
				flag=true;
			}
			if(bitMap==null)
			{
				bitMap=new BitSet();
			}
		}
		return bitMap;
	}
	
	// INSERT helpers
	
	//Helpers for insertIntoTable
	private static void insertAndShiftDown(String strTableName,int[] pageAndLocation,Object [] record)
	{                                                                                 //ana 8ayart hena 3ashan local date dih java.time we7na 3yzeen java.util
		//record[0]=java.time.LocalDate.now();
		record[0]=new Date();
		int page=pageAndLocation[0];
		int location=pageAndLocation[1];
		Object [] variable=record;
		
		while(variable!=null)
		{
			Table myTable=null;
			
			if(new File("./data/"+strTableName+"-"+page).exists())
				myTable=readTable(strTableName, page);
			else
				myTable=new Table();
			
			myTable.table.add(location, variable);
			variable=null;
			
			int originalPage=page;
			if(myTable.table.size()>maxSizeOfPage)
			{
				variable=myTable.table.remove(maxSizeOfPage);
				location=0;
				page++;
			}
			writeTable(strTableName, originalPage, myTable);
		}
	}
	
	
	//Helpers for updateTable
	private static int getKeyIndex(ArrayList<String[]>metadata)
	{
		int index=-1;
		
		for(String[]tuple:metadata)
		{
			index++;
			if(tuple[3].equals("true"))
				return index;
		}
		return -1;
	}
	
	// UPDATE helpers
	public static void updateClustring(String strTableName,String strKey,Hashtable<String,Object>htblColNameValue)throws DBAppException
	{
		ArrayList<String[]> relatedMetaData=getRelatedMetaData(strTableName);
		ArrayList<Integer> indexesLocations= getIndexLocation(getRelatedMetaData(strTableName));
		int keyindex= getKeyIndex(relatedMetaData);
		Object[] toBeInserted=toObjectArray(strTableName, htblColNameValue);
		if(!indexesLocations.contains(keyindex))//the key has no index must search manualy
			for (int i = 0; new File("./data/"+strTableName+"-"+i).exists(); i++)//finding page
			{
				Table table=readTable(strTableName, i);
				Vector<Object[]>actualTable=table.table;
				for(int j=0;j<actualTable.size();j++) //finding tuple 
				{
					Object [] tuple=actualTable.get(j);
					String key=tuple[keyindex]+"";
					if(key.equals(strKey))
					{	
						Hashtable<String, Object>oldTuple=new Hashtable<>();
						Hashtable<String, Object>newTuple=new Hashtable<>();
						for(int s=1;s<tuple.length;s++)
						{
							oldTuple.put(relatedMetaData.get(s)[1], (Object)tuple[s]);
							if(toBeInserted[s]!=null)
								newTuple.put(relatedMetaData.get(s)[1], (Object)toBeInserted[s]);
							else
								newTuple.put(relatedMetaData.get(s)[1],(Object) tuple[s]);
								
						}
						try
						{
							deleteFromTable(strTableName, oldTuple);
							insertIntoTable(strTableName, newTuple);
						}catch(DBAppException e)
						{
							insertIntoTable(strTableName, oldTuple);
							throw e;
						}
					}
				}
			}else// we can search by the bitmap index
			{
				//search the unique values for the one that corresponds our key
				final String colname =relatedMetaData.get(keyindex)[1];
				File dir = new File("./data/");
				File[] foundFiles = dir.listFiles(new FilenameFilter() {
				    public boolean accept(File dir, String name) {
				        return name.startsWith(strTableName+"-"+colname+"-"); //&& name.indexOf("-",arrSQLTerms[0].strTableName.length()+1)!=-1;
				    }
				});
				int x[]=bsInsertUpdate(strKey, strTableName, relatedMetaData.get(keyindex)[1],foundFiles.length-1);
				int page=x[0];
				int row=x[1];
				//IndexPage index = readCompressed(strTableName, relatedMetaData.get(keyindex)[1], x[3]);
				Table table=readTable(strTableName, page);
				Object[] tuple=table.table.get(row);
				if(!strKey.equals(tuple[keyindex]+""))
				{
					throw new DBAppException("this value is not present");
				}
				
				Hashtable<String, Object>oldTuple=new Hashtable<>();
				Hashtable<String, Object>newTuple=new Hashtable<>();
				for(int s=1;s<tuple.length;s++)
				{
					//System.out.print(tuple[s]+ "  ");
					oldTuple.put(relatedMetaData.get(s)[1], (Object)tuple[s]);
					if(toBeInserted[s]!=null)
						newTuple.put(relatedMetaData.get(s)[1], (Object)toBeInserted[s]);
					else
						newTuple.put(relatedMetaData.get(s)[1],(Object) tuple[s]);
						
				}
				try {
					deleteFromTable(strTableName, oldTuple);
					insertIntoTable(strTableName, newTuple);
				}
				catch(Exception e)
				{
					insertIntoTable(strTableName, oldTuple);
					throw e;
				}
			}						
		}

	public static void updateNonClustring(String strTableName,String strKey,Hashtable<String,Object>htblColNameValue)throws DBAppException
	{
		ArrayList<String[]> relatedMetaData=getRelatedMetaData(strTableName);
		ArrayList<Integer> indexesLocations= getIndexLocation(getRelatedMetaData(strTableName));
		int keyindex= getKeyIndex(relatedMetaData);
		Object[] toBeInserted=toObjectArray(strTableName, htblColNameValue);
		int count=-1;
		if(!indexesLocations.contains(keyindex))//the key has no index must search manualy
			for (int i = 0; new File("./data/"+strTableName+"-"+i).exists(); i++)//finding page
			{
				Table table=readTable(strTableName, i);
				for(int j=0;j<table.table.size();j++) //finding tuple 
				{
					count++;
					Object [] tuple=table.table.get(j);
					String key=tuple[keyindex]+"";
					if(key.equals(strKey))
					{	
						for(int s=1;s<tuple.length;s++)
						{
							if(toBeInserted[s]!=null && toBeInserted[s].getClass().getName().equals(relatedMetaData.get(s)[2]))
								{
									if(!indexesLocations.contains(s))//s isnt a column that's indexed
										tuple[s]=toBeInserted[s];
									else//s is an indexed column and its bitmap should be updated
									{
										String colName=relatedMetaData.get(s)[1];
										IndexPage index=null;
										boolean exists=false;
										for(int t=0;(index=readCompressed(strTableName, colName, t))!=null;t++)
										{
											for(int unq=0;unq<index.uniqueValues.size();unq++)
											{
												if(index.uniqueValues.get(unq).equals(toBeInserted[s]))
													{
														index.bitMaps.get(unq).set(count, 1);
														exists=true;
													}
												else
													index.bitMaps.get(unq).set(count, 0);
													
											}
											writeCompressed(strTableName, colName, t, index);
										}
										if(!exists)//bitmap doesnt exist has to be added
										{
											int[] pageAndLocation=getUniqueValueLocation(strTableName, colName, toBeInserted[s]);
											insertValueIntoIndex(strTableName, colName, pageAndLocation, count, toBeInserted[s]);
											removeExtraZeroes(strTableName, colName, count);
											removeEmptyBitMaps(strTableName, colName);
										}
										
									}
									tuple[s]=toBeInserted[s];
								}
							else if(  toBeInserted[s]!=null &&! toBeInserted[s].getClass().getName().equals(relatedMetaData.get(s)[2]))
								throw new DBAppException("incompatible types, "+toBeInserted[s].getClass().getName()+" & "+relatedMetaData.get(s)[2]);
						
						}
						
						tuple[0]=new Date();
						writeTable(strTableName, i, table);
					}
				}
			}else// we can search by the bitmap index
			{
				final String colname =relatedMetaData.get(keyindex)[1];
				File dir = new File("./data/");
				File[] foundFiles = dir.listFiles(new FilenameFilter() {
				    public boolean accept(File dir, String name) {
				        return name.startsWith(strTableName+"-"+colname+"-"); //&& name.indexOf("-",arrSQLTerms[0].strTableName.length()+1)!=-1;
				    }
				});
				int x[]=bsInsertUpdate(strKey, strTableName, relatedMetaData.get(keyindex)[1],foundFiles.length-1);
				int page=x[0];
				int row=x[1];
				Table table=readTable(strTableName, page);
				Object[] tuple=table.table.get(row);
				//IndexPage index = readCompressed(strTableName, relatedMetaData.get(keyindex)[1], x[3]);
				if(!strKey.equals(tuple[keyindex]+""))
				{
					throw new DBAppException("this value is not present");
				}
				for(int s=1;s<tuple.length;s++)
				{
					if(toBeInserted[s]!=null && toBeInserted[s].getClass().getName().equals(relatedMetaData.get(s)[2]))
						{
							if(!indexesLocations.contains(s))//s isnt a column that's indexed
								tuple[s]=toBeInserted[s];
							else//s is an indexed column and its bitmap should be updated
							{
								String colName=relatedMetaData.get(s)[1];
								IndexPage index2=null;
								boolean exists=false;
								for(int t2=0;(index2=readCompressed(strTableName, colName, t2))!=null;t2++)
								{
									for(int unq=0;unq<index2.uniqueValues.size();unq++)
									{
										if(index2.uniqueValues.get(unq).equals(toBeInserted[s]))
											{
												index2.bitMaps.get(unq).set(row, 1);
												exists=true;
											}
										else
											index2.bitMaps.get(unq).set(row, 0);
											
									}
									writeCompressed(strTableName, colName, t2, index2);
								}
								if(!exists)//bitmap doesnt exist has to be added
								{
									int[] pageAndLocation=getUniqueValueLocation(strTableName, colName, toBeInserted[s]);
									insertValueIntoIndex(strTableName, colName, pageAndLocation, row, toBeInserted[s]);
									removeExtraZeroes(strTableName, colName, row);
									removeEmptyBitMaps(strTableName, colName);
								}
								
								tuple[s]=toBeInserted[s];
							}
							
						}
					else if(  toBeInserted[s]!=null &&! toBeInserted[s].getClass().getName().equals(relatedMetaData.get(s)[2]))
						throw new DBAppException("incompatible types, "+toBeInserted[s].getClass().getName()+" & "+relatedMetaData.get(s)[2]);
				
				}
					writeTable(strTableName, page, table);	
				}
									
								
							
						
					
				
			}
	
	public static void removeExtraZeroes(String strTableName, String strColName, int bitLocation)
	{
		IndexPage index=null;
		for (int i = 0; (index=readCompressed(strTableName, strColName, i))!=null; i++)
		{
			for(int b=0;b<index.bitMaps.size();b++)
			{
				index.bitMaps.get(b).remove(bitLocation+1);
				
				if(! index.bitMaps.get(b).contains(1) )
				{
					index.bitMaps.remove(b);
					index.uniqueValues.remove(b);
					b--;
				}
			}
			writeCompressed(strTableName, strColName, i, index);
			
		}
	}
	public static void removeEmptyBitMaps(String strTableName, String strColName)
	{
		IndexPage index=null;
		boolean deleted=false;
		for (int i = 0; (index=readCompressed(strTableName, strColName, i))!=null; i++)
		{
			for(int b=0;b<index.bitMaps.size();b++)
			{
				if(! index.bitMaps.get(b).contains(1) )
				{
					index.bitMaps.remove(b);
					index.uniqueValues.remove(b);
					b--;
				}
			}
			if(index.uniqueValues.size()>0 && !deleted)
				writeCompressed(strTableName, strColName, i, index);
			else if(index.uniqueValues.size()>0 && deleted)
				{
					writeCompressed(strTableName, strColName, i, index);
					new File("./data/"+strTableName+"-"+strColName+"-"+(i+1)).delete();
				}
			else if(index.uniqueValues.size()==0 && !deleted)
			{
				new File("./data/"+strTableName+"-"+strColName+"-"+i).delete();
				i--;
				deleted=true;
			}
		}
	}
	
	//Helpers for deleteFromTable
	private static ArrayList<Integer>getIndexLocation(ArrayList<String[]>relatedMetaData)
	{//returns the location of the indexed objects in the table :D
		ArrayList<Integer>locations=new ArrayList<>();
		
		for(int i=0;i<relatedMetaData.size();i++)
		{
			if(relatedMetaData.get(i)[4].equals("true"))
				locations.add(i);
		}
		
		return locations;
	}
	
	//Helpers for CreateBitmapIndex
	private static void writeCompressed(String strTableName,String strColName,int i,IndexPage myTable)
	{
		try
		{
			File f=new File("./data/"+strTableName+"-"+strColName+"-"+i);
			ObjectOutputStream compressedOutput = new ObjectOutputStream(new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(f))));
			compressedOutput.writeObject(myTable);
			compressedOutput.close();
		}catch (IOException e) {e.printStackTrace();}
	}
	private static IndexPage readCompressed(String strTableName,String strColName,int i)
	{
		IndexPage myTable=null;
		try {			
			File f=new File("./data/"+strTableName+"-"+strColName+"-"+i);
			if(!f.exists())
				return null;
			ObjectInputStream compressedInput = new ObjectInputStream(new BufferedInputStream(new GZIPInputStream(new FileInputStream(f))));
			Object moObject=compressedInput.readObject();
			myTable=(IndexPage)moObject;
			compressedInput.close();
		}
		catch (IOException | ClassNotFoundException e) {e.printStackTrace();}
		return myTable;
	}
	private static int [] getUniqueValueLocation(String strTableName,String strColName,Object val) throws DBAppException
	{
		/*
		 * This method returns an array where location 0 is pageNumber and location 1 is locationInPage
		 */
		int [] sol=new int[2];
		sol[0]=-1;
		for(int i=0;new File("./data/"+strTableName+"-"+strColName+"-"+i).exists();i++)
		{
			sol[0]=i;
			IndexPage myTable=readCompressed(strTableName, strColName, i);
			for(int j=0;j<myTable.uniqueValues.size();j++)
			{
				int testValue=compare(val,myTable.uniqueValues.get(j));
				if(testValue<=0)
				{
					sol[1]=j;
					return sol;
				}
			}
			if(myTable.uniqueValues.size()<maxSizeOfIndex)
			{
				sol[0]=i-1;
				sol[1]=myTable.uniqueValues.size();
			}
			//if there is no next page and this page still has space
		}
		sol[0]++;
		return sol;
	}
	private static void updateMetaData(ArrayList<String[]> metaData)
	{
		String allData="Table Name,Column Name,Column Type,Key,Indexed"+"\n";
		for(String [] i:metaData)
		{
			for(int j=0;j<i.length;j++)
			{
				allData+=i[j];
				if(j!=i.length-1)
					allData+=",";
			}
			allData+="\n";
		}
		try 
		{
			PrintWriter pw=new PrintWriter(new FileWriter(new File("./data/metadata.csv")));
			pw.print(allData);
			pw.close();
		} 
		catch (Exception e) {e.printStackTrace();}
	}
	private static void insertValueIntoIndex(String strTableName, String strColName, int pageAndLocation [],int bitNumber,Object valueToInsert) throws DBAppException
	{
		/*
		 * This method is used to insert values to an index
		 * it gets the page of the index and location of uniqueWord
		 * then adds zeros to the end of all other unique words and 1 in the given Unique word
		 * if the given unique word doesn't exist it inserts the new word in the correct place
		 */
		int numberOfZeros=0;
		ArrayList<Integer> overFlowBits=null;
		Object overFlowUniqueValue=null;
		boolean inserted=false;
		for(int i=0;new File("./data/"+strTableName+"-"+strColName+"-"+i).exists();i++)
		{
			IndexPage indPage=readCompressed(strTableName, strColName, i);
			if(i==0)
				numberOfZeros=indPage.bitMaps.get(0).size()+1;
			if(overFlowUniqueValue!=null)
			{
				indPage.bitMaps.add(0,overFlowBits);
				indPage.uniqueValues.add(0,overFlowUniqueValue);
				if(indPage.uniqueValues.size()>maxSizeOfIndex)
				{
					overFlowBits=indPage.bitMaps.remove(indPage.bitMaps.size()-1);
					overFlowUniqueValue=indPage.uniqueValues.remove(indPage.uniqueValues.size()-1);
				}
				else
				{
					overFlowBits=null;
					overFlowUniqueValue=null;
				}
			}
			for(int j=0;j<indPage.bitMaps.size();j++)
			{
				if(!inserted && i==pageAndLocation[0] && j==pageAndLocation[1])
				{
					if(compare(indPage.uniqueValues.get(j), valueToInsert)!=0)
					{
						indPage.uniqueValues.add(j,valueToInsert);
						ArrayList<Integer> toAdd=zeros(numberOfZeros);
						toAdd.set(bitNumber, 1);
						indPage.bitMaps.add(j,toAdd);
						if(indPage.uniqueValues.size()>maxSizeOfIndex)
						{
							overFlowBits=indPage.bitMaps.remove(indPage.bitMaps.size()-1);//overflowbits and overflowunique value are not updated(didnt insert the zero bit)
							overFlowUniqueValue=indPage.uniqueValues.remove(indPage.uniqueValues.size()-1);
						}
					}
					else
						indPage.bitMaps.get(j).add(bitNumber, 1);
					inserted=true;
				}
				else
					indPage.bitMaps.get(j).add(bitNumber, 0);
			}
			if(overFlowUniqueValue!=null && !new File("./data/"+strTableName+"-"+strColName+"-"+(i+1)).exists())
			{
				IndexPage indPagetmp=new IndexPage(new ArrayList<Object>(),new ArrayList<ArrayList<Integer>>());
				indPagetmp.uniqueValues.add(overFlowUniqueValue);
				indPagetmp.bitMaps.add(overFlowBits);
				writeCompressed(strTableName, strColName, i+1, indPagetmp);
				overFlowBits=null;
				overFlowUniqueValue=null;
			}
				
			writeCompressed(strTableName, strColName, i, indPage);
		}
		if(!inserted)
		{
			IndexPage indPage=readCompressed(strTableName, strColName, pageAndLocation[0]);
			if(indPage==null)
				indPage=new IndexPage(new ArrayList<Object>(),new ArrayList<ArrayList<Integer>>());
			indPage.uniqueValues.add(valueToInsert);
			ArrayList<Integer> toAdd=zeros(numberOfZeros);
			if(toAdd.isEmpty())
				toAdd.add(1);
			else
				toAdd.set(bitNumber, 1);
				indPage.bitMaps.add(toAdd);
			writeCompressed(strTableName, strColName, pageAndLocation[0], indPage);
		}
	}
	private static ArrayList<Integer> zeros(int size) 
	{
		ArrayList<Integer> ans=new ArrayList<Integer>();
		while(size-->0)
			ans.add(0);
		return ans;
	}

	private static int getLocationOf(String strTableName,int[] place)
	{
		int counter=0;
		for (int i = 0; new File("./data/"+strTableName+"-"+i).exists(); i++)
		{
			Table table=readTable(strTableName, i);
			for(int j=0;j<table.table.size();j++)
			{
				if(i==place[0] && j==place[1])
					return counter;
				counter++;
			}
		}
		return -1;
	}
}