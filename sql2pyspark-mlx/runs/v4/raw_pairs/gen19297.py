from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city", "country")
    .pivot("segment", ["Corporate", "Consumer", "SMB"])
    .agg(F.sum("credit_limit").alias("total"), F.count(F.lit(1)).alias("item_count"))
)
