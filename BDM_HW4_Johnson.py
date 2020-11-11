from pyspark import SparkContext, SparkConf 
import sys
from pyspark.sql.types import *
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import year, month, dayofmonth, lower, col, round

if __name__ == "__main__":
    #create session context
    spark = SparkSession.builder.master("local[*]").appName("Pyspark").getOrCreate()
    sc = spark.sparkContext
    #input/output paths
    inputPath = sys.argv[1]
    outputPath = sys.argv[2] if len(sys.argv)>2 else 'output'
    #csv schema
    schema = StructType([StructField("date_received",TimestampType(),nullable=True),
                         StructField("product",StringType(),True), 
                         StructField("sub_product",StringType(),True), StructField("issue", StringType(), True), 
                         StructField("sub_issue", StringType(), True),
                         StructField("consumer_complaint_narrative", StringType(), True),
                         StructField("company",StringType(),True), 
                         StructField("state", StringType(), True), 
                         StructField("zip_code", StringType(), True),
                         StructField("tags", StringType(), True), 
                         StructField("consumer_consent_provided", StringType(), True), 
                         StructField("submitted_via", StringType(), nullable=True), 
                         StructField("date_send_to_company", StringType(), nullable=True), 
                         StructField("company_response_to_consumer", StringType(), nullable=True), 
                         StructField("timely_response", StringType(), nullable=True), 
                         StructField("consumer_disputed", StringType(), nullable=True), 
                         StructField("compaint_id", StringType(), nullable=True)
                         ])
    #transformations   
    complaints = spark.read.option("header","true").csv(path=inputPath, schema=schema)
    result1DF = complaints.filter("product is not null").filter("date_received is not null").filter("company is not null").filter("issue is not null").select(lower(col("product")).alias("product"), year("date_received").alias("year"), "issue", "company")
    totalComplainnts = result1DF.groupBy("Product", "year").count().select(col("Product").alias("complaintsProduct"), col("year").alias("complaint_year"), col("count").alias("count"))
    totalCompanies = result1DF.groupBy("Product", "year", "company").count().select("Product", "year", col("count").alias("count_company"))
    inner = totalComplainnts.join(totalCompanies, (totalComplainnts.complaintsProduct == totalCompanies.Product) & (totalComplainnts.complaint_year == totalCompanies.year),how='inner').select("Product", "year", "count", "count_company")
    df = inner.groupBy("Product", "year", "count").max("count_company").withColumn("percentage", round(col("max(count_company)") * 100/ col("count"))).select("Product", "year", "count", col("max(count_company)").alias("count_company"), "percentage")
    df3 = df.selectExpr("Product","year","count","count_company","cast(percentage as int) percentage")
    print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
    df3.show()

    #sort helper function strips first
    def sortRDD(x):
        return (x[0].strip(), x[1],x[2], x[3],x[4])

    sor = df3.rdd.map(lambda x : sortRDD(x))
    columns = ["Product","year","count","count_company","percentage"]
    orderdRDD = sor.toDF(columns)
    sortedDF = orderdRDD.orderBy("Product")
    #adder helper function
    def add(x):
        if(',' in x[0]):
            a = "{}".format(x[0])
            return (a.strip(), x[1],x[2], x[3],x[4])
        else:
            return (x[0], x[1],x[2], x[3],x[4])

    rdd = sortedDF.rdd.map(lambda x : add(x))
    
    columns = ["Product","year","count","count_company","percentage"]
    finalResult = rdd.toDF(columns)
    finalResult.rdd.map(lambda x: ",".join(map(str, x))).coalesce(1).saveAsTextFile(outputPath)
