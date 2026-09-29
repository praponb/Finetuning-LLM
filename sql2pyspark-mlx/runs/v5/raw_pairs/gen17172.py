from pyspark.sql import functions as F

result = (
    df_shipments
    .select(F.col("delivered_date").alias("event_date"))
    .unionByName(df_inventory.select(F.col("last_restock").alias("event_date")))
    .distinct()
)
