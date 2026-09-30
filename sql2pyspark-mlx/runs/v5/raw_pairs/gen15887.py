from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("age") == 55) | (F.col("city") == "Berlin"))
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("segment") == "Consumer", 1).otherwise(0)).alias("consumer_count"),
        F.sum(F.when(F.col("country") == "SG", 1).otherwise(0)).alias("sg_count"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
    )
    .orderBy(F.col("sg_count").desc())
)
