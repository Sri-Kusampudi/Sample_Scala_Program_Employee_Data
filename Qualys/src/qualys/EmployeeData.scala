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


object EmployeeData extends App{

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


    val source = Source.fromFile("emp.txt" )
    val list = source.getLines.toList
    
    //creating a clean file
    val file = new File("empclean.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    for (line <- list) {
      val lineIs = line.map(str => Option(str).getOrElse(""))
      bw.write(line.replace(" ", "").replace("\",\"", "|").replace(",","|").replace("\"","")
          .replace("\t","").concat("\n"))
    }
    bw.close()

    //loading from clean file
    val schemaIs = StructType(Array(
            StructField("name", StringType, true),
            StructField("age", StringType, true),
            StructField("salary", StringType, true),
            StructField("benefits", StringType, true),
            StructField("department", StringType, true)))
        
    val df = sqlContext.read.format("com.databricks.spark.csv")
                             .option("delimiter", "|")
                             .option("quote","\"")
                             .option("space", "")
                             .option("inferSchema", "false")
                             .option("header", "true")
                             .option("badRecordsPath", "quarantine.txt")
                             .schema(schemaIs)
                             .load("empclean.txt")

    df.show()
    val originalSchema = df.schema

    //validating and capturing errors
    def validateColumns(row: Row): Row = {
    
    var err_col: String = null
    var err_val: String = null
    var err_desc: String = null


    try {
        val name = row.getAs[String]("name")
        val age = row.getAs[String]("age")
        val salary = row.getAs[String]("salary")
        val benefits = row.getAs[String]("benefits")
        val department = row.getAs[String]("department")
        val formatter = java.text.NumberFormat.getCurrencyInstance
        val salary1 = formatter.format(row.getAs[String]("salary").toDouble)
        //if (!(row.getAs[String]("salary").matches("[-+]?\\d+(\\.\\d+)?"))){ err_col = "error"}
        if (row.getAs[String]("department").isEmpty() ){ }
    } catch {
        case e: NumberFormatException => {
          err_col = "salary"
          err_desc = "Number Format Error"
          err_val = row.getAs[String]("salary")
        }
        case e: NullPointerException => {
          err_col = "department"
          err_desc = "department is empty"
          err_val = ""
        }
    }

    Row.merge(row, Row(err_col),Row(err_val),Row(err_desc))
    }

    // Modify you existing schema with you additional metadata fields
    val newSchema = originalSchema.add("ERROR_COLUMN", StringType, true)
                                      .add("ERROR_VALUE", StringType, true)
    								  .add("ERROR_DESCRIPTION", StringType, true)
 	 
    								  
    val validateDF = df.rdd.map{row => validateColumns(row)}	
    
    // Reconstruct the DataFrame with additional columns					  
    val checkedDf = sqlContext.createDataFrame(validateDF, newSchema)
    //checkedDf.show()
    
    // Filter out row having errors
    val errorDf = checkedDf.filter("ERROR_COLUMN is not null")
    errorDf.show()
    errorDf.sort(asc("name")).coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("error.txt")
    // Filter out row having no errors
    val errorFreeDf = checkedDf.filter("ERROR_COLUMN is null")
    errorFreeDf.show()
    val actualDf = errorFreeDf.sort(asc("name"))
    actualDf.select("name", "age", "salary", "benefits", "department").coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("output.txt")
}