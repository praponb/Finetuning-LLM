from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment") == "Consumer")
    .groupBy("city")
    .agg(F.avg("age").alias("avg_age"))
    .filter(F.col("avg_age") > 46)
    .select("city", "avg_age")
    .orderBy(F.col("avg_age").desc(), "city")
)
