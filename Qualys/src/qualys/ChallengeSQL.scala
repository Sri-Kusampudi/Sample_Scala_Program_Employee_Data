package qualys
import org.apache.spark._

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.log4j._
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import scala.io.Source
import java.io._


object ChallengeSQL extends App{

    Logger.getLogger("org").setLevel(Level.ERROR)
    //Initializing Spark Context configuration
    val conf = new SparkConf().set("spark.sql.warehouse.dir", "file:///C:/SamplePrograms/Qualys/").setAppName("textfileReader")
    //setting master as the local mode.
    conf.setMaster("local")
    //Creating Spark Context
    val sc = new SparkContext(conf)
    //Creating Sql Context
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    //Setting the Hadoop Home
    System.setProperty("hadoop.home.dir", "C:\\winutils");

    //Setting the Header for the file
    val schemaString = "empno,ename,job,mgr,hiredate,sal,comm,deptno"
        
    val employees = sqlContext.read.format("csv")
             .option("delimiter",",")
             .option("header", "true")
             .load("employes.csv")
        //For validating
      employees.show()
      
      val depts = sqlContext.read.format("csv")
             .option("delimiter",",")
             .option("header", "true")
             .load("dept.csv")
        //For validating
      depts.show()

      employees.registerTempTable("employees")
      depts.registerTempTable("depts")
      
      sqlContext.sql("select e.empno,e.ename,e.job,e.mgr,e.hiredate,e.sal,e.comm,d.deptno,d.dname,d.loc" +
	               " from employees e inner join depts d" +          
                 " where e.deptno = d.deptno").show()
    /*1. Write a query and compute average salary (sal) of employees distributed by location (loc). 
     * Output shouldn't show any locations which don't have any employees.*/
      sqlContext.sql("select avg(e.sal),d.loc" +
	               " from employees e inner join depts d" +          
                 " where e.deptno = d.deptno" +
                 " group by d.loc").show()
      
/*2. Write a query and compute average salary (sal) of employees located in NEW YORK excluding PRESIDENT */
                 
     sqlContext.sql("select avg(e.sal),d.loc" +
     " from employees e inner join depts d" +          
     " where e.deptno = d.deptno" +
     " and d.loc = 'NEW YORK'" +
     " and e.job  != 'PRESIDENT'" +
     " group by d.loc").show()
/*3. Write a query and compute average salary (sal) of four most recently hired employees */
     sqlContext.sql("select avg(sal)from (select sal from employees sort by hiredate desc LIMIT 4)").show()
     
     /*4. Write a query and compute minimum salary paid for different kinds of jobs in DALLAS
   */
     
      sqlContext.sql("select min(cast(e.sal as double)),e.job" +
     " from employees e inner join depts d" +          
     " where e.deptno = d.deptno" +
     " and d.loc = 'DALLAS'" +
     " group by e.job").show()
}