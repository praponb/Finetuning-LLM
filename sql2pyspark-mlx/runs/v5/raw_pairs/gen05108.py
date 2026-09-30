from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") <= 77)
    .select(F.col("shipped_date").alias("event_date"))
    .intersect(df_store_sales.select(F.col("sale_date").alias("event_date")))
)
