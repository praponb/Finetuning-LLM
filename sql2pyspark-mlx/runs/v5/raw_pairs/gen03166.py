from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.region") == "Central")
    .groupBy(F.col("s.id"), F.col("s.store_name"))
    .agg(
        F.count(F.col("i.id")).alias("inventory_count"),
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
        F.count(F.col("i.quantity")).alias("quantity_count"),
    )
)
