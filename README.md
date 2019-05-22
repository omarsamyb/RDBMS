# RDBMS
A RDBMS with bitmap index support simulation with Java.

Supported RDBMS features:
* Create Table
* Insert into Table
* Update Table
* Delete from Table
* Create Bitmap Index
* Select from Table with support for ANDing, ORing & XORing conditions taking into consideration the Precedence of the operators.

Here is an example code that creates a table, creates an index, does few inserts, and a select.
```
String strTableName = "Student";
Hashtable htblColNameType = new Hashtable( );
htblColNameType.put("id", "java.lang.Integer");
htblColNameType.put("name", "java.lang.String");
htblColNameType.put("gpa", "java.lang.double");
createTable( strTableName, "id", htblColNameType );

createBitmapIndex( strTableName, "gpa" );

Hashtable htblColNameValue = new Hashtable( );
htblColNameValue.put("id", new Integer( 2343432 ));
htblColNameValue.put("name", new String("Ahmed Noor" ) );
htblColNameValue.put("gpa", new Double( 0.95 ) );
insertIntoTable( strTableName , htblColNameValue );

htblColNameValue.clear( );
htblColNameValue.put("id", new Integer( 453455 ));
htblColNameValue.put("name", new String("Ahmed Noor" ) );
htblColNameValue.put("gpa", new Double( 0.95 ) );
insertIntoTable( strTableName , htblColNameValue );

htblColNameValue.clear( );
htblColNameValue.put("id", new Integer( 5674567 ));
htblColNameValue.put("name", new String("Dalia Noor" ) );
htblColNameValue.put("gpa", new Double( 1.25 ) );
insertIntoTable( strTableName , htblColNameValue );

htblColNameValue.clear( );
htblColNameValue.put("id", new Integer( 23498 ));
htblColNameValue.put("name", new String("John Noor" ) );
htblColNameValue.put("gpa", new Double( 1.5 ) );
insertIntoTable( strTableName , htblColNameValue );

htblColNameValue.clear( );
htblColNameValue.put("id", new Integer( 78452 ));
htblColNameValue.put("name", new String("Zaky Noor" ) );
htblColNameValue.put("gpa", new Double( 0.88 ) );
insertIntoTable( strTableName , htblColNameValue );

SQLTerm[] arrSQLTerms;
arrSQLTerms = new SQLTerm[2];
arrSQLTerms[0]._strTableName = "Student";
arrSQLTerms[0]._strColumnName= "name";
arrSQLTerms[0]._strOperator = "=";
arrSQLTerms[0]._objValue = "John Noor";
arrSQLTerms[1]._strTableName = "Student";
arrSQLTerms[1]._strColumnName= "gpa";
arrSQLTerms[1]._strOperator = "=";
arrSQLTerms[1]._objValue = new Double( 1.5 );
String[]strarrOperators = new String[1];
strarrOperators[0] = "OR";

// select * from Student where name = “John Noor” or gpa = 1.5;
Iterator resultSet = selectFromTable(arrSQLTerms , strarrOperators);
```
