from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.count("*").alias("inventory_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.min_quantity") > 180)
    .select(
        F.col("s.opened_date"),
        F.col("ss.min_quantity"),
        F.col("ss.inventory_count"),
    )
    .orderBy("opened_date")
)
