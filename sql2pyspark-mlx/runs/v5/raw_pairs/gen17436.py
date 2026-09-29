from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 3, 6]))
    .select("product_id", "store_id", F.round("quantity", 2).alias("quantity_rounded"))
    .orderBy("quantity_rounded")
)
