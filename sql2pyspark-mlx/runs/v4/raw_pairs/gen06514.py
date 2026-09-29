from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.filter(F.col("product_id").isin([3, 4, 5, 7])).alias("i"), F.col("s.id") == F.col("i.store_id"), "left_anti")
    .select(F.col("s.id"), F.col("s.region"), F.col("s.city"))
    .orderBy("city")
)
