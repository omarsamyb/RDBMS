//package DataBucketsII;
//
//import java.io.File;
//import java.util.ArrayList;
//import java.util.BitSet;
//
//public class tt extends DBApp{
//	public static int bss(int x, int[]y)
//	{
//	    int left = 0;
//		int right = y.length;
//
//	    while (left < right)
//	    {
//	        int m = (left+right)/2;
//	        if (y[m] >= x) // this is the important part:
//	                      // even if we find it, we continue,
//	                      // so we find the first such value.
//	            right = m;
//	        else
//	            left=m+1;
//	    }
//	    return left;
//	}
//	public static int bs (SQLTerm arrSQLTerms, int lastPageIndex) throws DBAppException
//	{
//		IndexPage indexPage=null;
//		BitSet bitMap=null;
//		ArrayList<Object> uniqueValues;
//		
//	    int leftT = 0;
//		int rightT = lastPageIndex+1;
//	    while (leftT<rightT)
//	    {
//	        int mT = (leftT+rightT)/2;
//	        indexPage = readCompressed(arrSQLTerms.strTableName, arrSQLTerms.strColumnName, mT);
//	        uniqueValues = indexPage.uniqueValues;
//	        int leftP = 0;
//	        int rightP = uniqueValues.size();
//	        while(leftP<rightP)
//	        {
//	        	int mP = (leftP+rightP)/2;
//	        	if(evaluate(uniqueValues.get(mP), ">=", arrSQLTerms.objValue))
//	        	{
//	        		rightP= mP;
//	        	}
//	        	else
//	        	{
//	        		leftP=mP+1;
//	        	}
//	        }
//	        if(leftP == 0 && evaluate(uniqueValues.get(0), "!=", arrSQLTerms.objValue))
//	        {
//	        	rightT = mT;
//	        }
//	        else if (leftP == 0 && evaluate(uniqueValues.get(0), "=", arrSQLTerms.objValue))
//	        {
//	        	
//	        }
//	        else if(leftP == uniqueValues.size())
//	        {
//	        	leftT = mT+1;
//	        }
//	        else
//	        {
//	        	
//	        }
//	    }
//	}
//	public static BitSet getBits (int startPage, int startTuple, SQLTerm arrSQLTerms, boolean forward) throws DBAppException
//	{
//		IndexPage indexPage=null;
//		BitSet bitMap=null;
//		boolean flag=false;
//		if(forward)
//		{
//			for(int i=startPage;new File("./data/"+arrSQLTerms.strTableName+"-"+arrSQLTerms.strColumnName+"-"+i).exists();i++)
//			{
//				indexPage = readCompressed(arrSQLTerms.strTableName, arrSQLTerms.strColumnName, i);
//				for(int j= (flag)?0:startTuple;j<indexPage.uniqueValues.size();j++)
//				{
//					if(evaluate(indexPage.uniqueValues.get(j), arrSQLTerms.strOperator, arrSQLTerms.objValue))
//					{
//						if(bitMap==null)
//						{
//							bitMap=convertToBitSet(indexPage.bitMaps.get(j));
//						}
//						else
//						{
//							bitMap.or(convertToBitSet(indexPage.bitMaps.get(j)));
//						}
//					}
//				}
//				flag=true;
//			}
//			if(bitMap==null)
//			{
//				bitMap=new BitSet();
//			}
//		}
//		else
//		{
//			flag=false;
//			for(int i=startPage;new File("./data/"+arrSQLTerms.strTableName+"-"+arrSQLTerms.strColumnName+"-"+i).exists();i--)
//			{
//				indexPage = readCompressed(arrSQLTerms.strTableName, arrSQLTerms.strColumnName, i);
//				for(int j=(flag)?indexPage.uniqueValues.size()-1:startTuple;j>=0;j--)
//				{
//					if(evaluate(indexPage.uniqueValues.get(j), arrSQLTerms.strOperator, arrSQLTerms.objValue))
//					{
//						if(bitMap==null)
//						{
//							bitMap=convertToBitSet(indexPage.bitMaps.get(j));
//						}
//						else
//						{
//							bitMap.or(convertToBitSet(indexPage.bitMaps.get(j)));
//						}
//					}
//				}
//				flag=true;
//			}
//			if(bitMap==null)
//			{
//				bitMap=new BitSet();
//			}
//		}
//		return bitMap;
//	}
//
//	public static void main(String[] args) {
//		int []x= {1,2,4,5,6,7};
//		System.out.println(bs(20000, x));
//	}
//
//}
