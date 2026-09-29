from pyspark.sql import functions as F

valid_inventory = (
    df_inventory
    .filter(((F.col("last_restock") > "2023-05-01") & F.col("store_id").isin([2, 3, 4, 5])) | F.col("product_id").isin([1, 5, 10]))
)

inventory_stats = (
    valid_inventory
    .groupBy(F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"))
    .agg(F.min("quantity").alias("min_quantity"))
)

result = (
    inventory_stats
    .filter(F.col("min_quantity") >= 160)
    .select("last_restock_month", "min_quantity")
    .orderBy("min_quantity")
)
