from pyspark.sql import functions as F

valid_inventory = (
    df_inventory
    .filter((F.col("quantity") > 113) & (F.col("last_restock") <= "2023-03-01") & (F.col("last_restock") >= "2023-10-01"))
)

summary = (
    valid_inventory
    .groupBy("store_id", "product_id")
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = (
    summary
    .select("store_id", "product_id", "unique_products", "quantity_range")
    .orderBy("unique_products")
)
