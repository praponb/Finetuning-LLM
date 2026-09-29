from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .pivot("segment", ["Corporate", "SMB"])
    .agg(F.max("age").alias("value_sum"), F.count(F.lit(1)).alias("item_count"))
)
