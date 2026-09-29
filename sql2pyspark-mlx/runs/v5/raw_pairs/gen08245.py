from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter(F.col("s.city") != "New York")
    .groupBy(F.col("s.id"), F.col("s.store_name"))
    .agg(
        F.count(F.col("i.id")).alias("inventory_count"),
        F.sum(F.col("i.quantity")).alias("total_quantity"),
    )
    .filter(F.col("total_quantity") > 1210)
    .orderBy("total_quantity", "inventory_count")
)
