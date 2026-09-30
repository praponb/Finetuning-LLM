from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.date_format("shipped_date", "yyyy-MM").alias("shipped_month"))
    .agg(F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_count"))
    .orderBy("shipped_month")
)
