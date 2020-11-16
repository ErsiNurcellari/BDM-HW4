from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as func
import sys

input_file = sys.argv[1] 
output_folder = sys.argv[2]

sc = SparkContext()

spark = SparkSession(sc)

sql_c = SQLContext(sc)

df1 = sql_c.read.csv(input_file, header=True)

#first ectract year from the date column
df1.select(func.split(func.col("Date received"),"/")[2].alias("Date received") , func.col("Product"), func.col("Company")).groupBy("Product","Date received","Company").agg(
     func.count(func.lit(1)).alias("Num Of Records")
   ).groupBy("Product","Date received").agg(
        func.sum("Num of Records").alias("Total"),#calculate total items sold
        func.countDistinct("Date received","Product","Company").alias('Distinct Companies'),##calculate disctinct items sold
        ((func.max("Num of Records")/func.sum("Num of Records"))*100).alias("Max %") #calculate %
    ).sort("Product","Date received").write.csv(output_folder) #sort based on required output and write to folder