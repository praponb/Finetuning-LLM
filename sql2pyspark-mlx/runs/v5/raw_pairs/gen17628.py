from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "right")
    .select(F.col("i.id"), F.col("i.product_id"), F.col("s.opened_date"))
    .orderBy("id")
)
