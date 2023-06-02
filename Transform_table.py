from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# create a SparkSession
spark = SparkSession.builder.appName("Transform").getOrCreate()

# read the CSV file from S3 bucket
df = spark.read.format("csv").option("header", True).load("s3://nc-fire-bucket/input/Fire_Department_Calls_for_Service.csv")

# Transform the data
new_cols = [col(c).alias(c.replace(" ", "_")) for c in df.columns]

df= df.select(new_cols) \
    .withColumn("City", 
                when(col("City").isin("SAN FRANCISCO", "SF", "SF\"", "SFO", ' S\"', '', '\"'), "San Francisco")
                .when(col("City").isin("FORT MASON", "FM", 'FM\"', ' FM\"'), "Fort Mason")
                .when(col("City").isin("TREASURE ISLAND", "Treasure Isla", "TI", " TI\""), "Treasure Island")
                .when(col("City").isin("YB", "YERBA BUENA IS", " YB\"", "YB\""), "Yerba Buena")
                .when(col("City").isin("DALY CITY", "DC", "DC\"", " DC\""), "Daly City")
                .when(col("City").isin("PR", "PR\"", "PRESIDIO", " PR\""), "Presidio")
                .when(col("City").isin("HP", "HP\"", " HP\"", "HUNTERS POINT"), "Hunters Point")
                .when(col("City").isin("BN", " BN\""), "Brisbane")
                .when(col("City").rlike(".*ST.*|.*AV.*|AI|OAK|.*Block.*"), "San Francisco")
                .otherwise(col("City"))
               ) \
    .withColumn("Original_Priority",
            when(col("Original_Priority").isin("A", "B", "C", "E", 'T', 'I', ' '), "1")
            .otherwise(col("Original_Priority"))
           ) \
    .withColumn("Fire_Prevention_District",
            when(col("Fire_Prevention_District").isin([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]), 
                        col("Fire_Prevention_District").cast("integer"))
            .when(col("Fire_Prevention_District").isin("None"), "1")
            .otherwise(col("Fire_Prevention_District"))
           ) \
    .withColumn("Zipcode_of_Incident", col("Zipcode_of_Incident").cast("integer")
           ) \
    .drop('Address', 'Transport_DtTm', 'Hospital_DtTm', 'Priority', 'Final_Priority', 'Available_DtTm', 
              'Unit_sequence_in_call_dispatch', 'Supervisor_District', 'Neighborhooods_-_Analysis_Boundaries',
              'Watch_Date', 'Analysis_Neighborhoods') \
    .na.drop()

# write the cleaned data as parquet to S3 bucket
df.repartition(1) \
    .write.format("parquet") \
    .option("header", True) \
    .mode("overwrite") \
    .save("s3://nc-fire-bucket/output/")

# stop the SparkSession
spark.stop()