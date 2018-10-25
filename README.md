# Sample_Scala_Program_Employee_Data

 Please find attached a Comma Separated Value File which needs to be cleaned. The file is called emp.txt. 

It has errors in it which are basically quotes and unnecessary spaces/tabs (leading and trailing spaces/tabs).

Write a Spark application using Scala or Java programming language, to clean the Comma Separated Value file to remove unnecessary spaces and quotes and apply the format to the digital values as it shown in the second test record. (Scala preferred).

The first line in the file is a header, to which the cleansing rule have to be applied as well. 

The second record specifies the column datatype that has to be used for DataFrame or Dataset of the Spark application and should be eliminated from the result output. 

Records do not match the header layout or datatype specified in the second test line have to be redirected to the quarantine.txt output file.

The transformed cleaned records should be stored to the output.txt file.

For your reference, the output and quarantine files are also attached. They are named "output.txt" and "quarantine.txt" respectively.
