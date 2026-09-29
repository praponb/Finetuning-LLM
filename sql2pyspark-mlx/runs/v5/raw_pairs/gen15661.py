from pyspark.sql import functions as F

result = (
    df_shipments
    .select(F.col("shipped_date").alias("event_date"))
    .subtract(df_inventory.select(F.col("last_restock").alias("event_date")))
    .orderBy("event_date")
)
