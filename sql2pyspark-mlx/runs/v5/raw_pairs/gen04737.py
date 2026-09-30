from pyspark.sql import functions as F

result = (
    df_inventory
    .select(F.col("last_restock").alias("event_date"), F.lit("inventory").alias("kind"))
    .unionByName(
        df_stores
        .filter(F.col("region").isin(["East", "North", "Central"]))
        .select(F.col("opened_date").alias("event_date"), F.lit("store").alias("kind"))
    )
    .distinct()
)
