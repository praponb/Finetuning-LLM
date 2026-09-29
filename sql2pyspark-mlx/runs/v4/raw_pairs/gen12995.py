from pyspark.sql import functions as F

filtered_inventory = (
    df_inventory
    .filter(F.col("last_restock") > "2024-01-01")
    .withColumn("last_restock_period", F.date_trunc("MONTH", "last_restock"))
)

agg = (
    filtered_inventory
    .groupBy("product_id", "store_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.count("*").alias("inventory_count"),
    )
)

result = (
    agg
    .select("product_id", "store_id", "max_quantity", "inventory_count")
    .orderBy(F.col("max_quantity").desc(), "product_id", "store_id")
    .limit(3)
)
