from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter((F.col("quantity") > 201) & (F.col("last_restock") <= "2024-06-30"))
    .withColumn("quantity_whole", F.ceil("quantity"))
)

inventory_stats = (
    recent_inventory
    .groupBy("store_id")
    .agg(F.max("last_restock").alias("latest_last_restock"))
)

result = (
    inventory_stats
    .select("store_id", "latest_last_restock")
    .orderBy(F.col("latest_last_restock").desc())
)
