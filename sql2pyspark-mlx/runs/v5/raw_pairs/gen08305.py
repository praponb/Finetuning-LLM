from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 5]))
    .select("product_id")
    .unionByName(df_inventory.filter(F.col("store_id") == 6).select("product_id"))
    .distinct()
    .orderBy("product_id")
)
