from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.filter(F.col("product_id").isin([2, 7, 8, 10])).alias("i"), F.col("s.id") == F.col("i.store_id"), "left_anti")
    .filter(F.col("s.city") == "Bangkok")
    .select(F.col("s.id"), F.col("s.sqft"))
    .orderBy("sqft")
)
