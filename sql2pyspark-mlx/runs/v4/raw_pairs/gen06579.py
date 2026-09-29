from pyspark.sql import functions as F

enriched = (
    df_inventory
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .select("e.*", F.col("s.city"), F.col("s.opened_date"))
)

result = (
    enriched
    .select("id", "store_id", "product_id", "quantity")
    .orderBy("store_id", "id")
)
