from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(F.col("sale_date").alias("event_date"))
    .unionByName(df_shipments.select(F.col("shipped_date").alias("event_date")))
    .orderBy("event_date")
)
