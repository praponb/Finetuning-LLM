from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"))
)
