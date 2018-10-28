# Sample_Scala_Program_Employee_Data

 1) Please find attached a Comma Separated Value File which needs to be cleaned. The file is called emp.txt. 

It has errors in it which are basically quotes and unnecessary spaces/tabs (leading and trailing spaces/tabs).

Write a Spark application using Scala or Java programming language, to clean the Comma Separated Value file to remove unnecessary spaces and quotes and apply the format to the digital values as it shown in the second test record. (Scala preferred).

The first line in the file is a header, to which the cleansing rule have to be applied as well. 

The second record specifies the column datatype that has to be used for DataFrame or Dataset of the Spark application and should be eliminated from the result output. 

Records do not match the header layout or datatype specified in the second test line have to be redirected to the quarantine.txt output file.

The transformed cleaned records should be stored to the output.txt file.

For your reference, the output and quarantine files are also attached. They are named "output.txt" and "quarantine.txt" respectively.

--------------------------------------------------------------------------------
2) This question expects you to write queries in Spark SQL / Hive to perform the following:
Please find Statements.sql file attached with statements to create and insert into the tables dept and emp. 
Also find two csv (emp.csv, dept.csv) files for your reference of how the tables emp, dept should look like after executing the statements within the statements.sql 

a. Write a query and compute average salary (sal) of employees distributed by location (loc). Output shouldn't show any locations which don't have any employees.

b. Write a query and compute average salary (sal) of employees located in NEW YORK excluding PRESIDENT

c. Write a query and compute average salary (sal) of four most recently hired employees

d. Write a query and compute minimum salary paid for different kinds of jobs in DALLAS
