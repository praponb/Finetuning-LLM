from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.filter(F.col("product_id") == 3).alias("i"), F.col("s.id") == F.col("i.store_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.opened_date"))
    .orderBy(F.col("id").desc())
    .limit(1)
)
