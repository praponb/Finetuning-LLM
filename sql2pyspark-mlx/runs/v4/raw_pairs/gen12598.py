from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") < 55)
    .withColumn("name_length", F.length("name"))
    .orderBy("name_length")
)
