from pyspark.sql import functions as F

filtered_inventory = df_inventory.filter((F.col("quantity") > 78) & (F.col("store_id") == 5))

agg = (
    filtered_inventory
    .groupBy("store_id")
    .agg(F.min("last_restock").alias("min_last_restock"))
)

result = agg.select("store_id", "min_last_restock")
