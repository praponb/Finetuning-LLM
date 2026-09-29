from pyspark.sql import functions as F

cleaned = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .withColumn("credit_limit_band", F.when(F.col("credit_limit") >= 14700, "High").when(F.col("credit_limit") >= 2700, "Medium").otherwise("Low"))
)

result = (
    cleaned
    .groupBy("city")
    .pivot("country", ["UK", "SG"])
    .agg(F.avg("credit_limit").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)
