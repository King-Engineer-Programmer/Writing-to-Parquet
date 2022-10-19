# Writing-to-Parquet


In this activity you'll determine the differences in query execution time between a temporary view of an original Spark DataFrame, a parquet DataFrame, and a partitioned parquet Spark DataFrame.
Instructions:


Open Google Colab, and create a new notebook.


Using the starter code provided, start a Spark session in your Colab notebook.


Import the DelayedFlights.csv file to a Spark DataFrame in your Colab notebook.


Create a temp view of your Spark DataFrame, and create a SparkSQL query that determines the total distance and flight count for each unique “Origin” and “Dest” combination.


Record the execution time of the SparkSQL query using the time.time() method.


Note: You may want to run this query twice to eliminate initial load time.




Save your DataFrame in parquet format, and reload the dataset into a new Spark DataFrame.


Create a new temp view from the parquet Spark DataFrame, and rerun the SparkSQL query that determines the total distance and flight count for each unique “Origin” and “Dest” combination.

Record the execution time of the SparkSQL query using the time.time() method.



Now, save your original Spark DataFrame again, but this time, save it in parquet format, using Origin as the column to partition by.


Reload your partitioned parquet dataset into a new Spark DataFrame.


Create another new temp view from the partitioned parquet Spark DataFrame, and rerun the SparkSQL query that determines the total distance and flight count for each unique Origin and Dest combination.

Record the execution time of the SparkSQL query using the time.time() method.



Compare the performance of all three SparkSQL queries based upon the underlying data format.

Did the partitioned parquet format perform better or worse than the non-partitioned format? Why or why not?



Try creating a SparkSQL query that filters the partitioned data where TailNum equals N712SW. Show the results for the Origin and TailNum fields.


Record the execution time of the SparkSQL query using the time.time() method.


Note: This query tries to filter on your partitioned Origin column.




Create a new SparkSQL query that filters the partitioned data where TailNum equals N712SW. Show the results for the Dest and TailNum fields.


Record the execution time of the SparkSQL query using the time.time() method.


Note: This query does not filter on your partitioned Origin column.




Compare the performance of your filter queries from a partitioned versus non-partitioned dataset.



Copyright 2022 2U. All Rights Reserved.
