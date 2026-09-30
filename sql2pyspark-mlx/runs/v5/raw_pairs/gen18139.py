from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") <= 184)
    .select(F.col("last_restock").alias("event_date"))
    .unionByName(
        df_shipments
        .filter(F.col("status") == "LOST")
        .select(F.col("delivered_date").alias("event_date"))
    )
    .distinct()
)
