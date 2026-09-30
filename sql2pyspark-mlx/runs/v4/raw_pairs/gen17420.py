from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") >= "2025-01-24") & F.col("order_id").isin([13, 23, 36]) & (F.col("order_id") == 36))
    .groupBy("status")
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") < 5)
)
