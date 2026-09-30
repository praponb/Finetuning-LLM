from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.filter(F.col("product_id").isin([3, 4, 7])).alias("i"), F.col("s.id") == F.col("i.store_id"), "left_semi")
    .filter(F.col("s.region") == "East")
    .select(F.col("s.id"), F.col("s.sqft"), F.col("s.region"), F.col("s.store_name"))
)
