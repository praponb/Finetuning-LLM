from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment", "city")
    .pivot("country", ["JP", "UK"])
    .agg(F.avg("credit_limit").alias("value_sum"), F.count(F.lit(1)).alias("n"))
)
