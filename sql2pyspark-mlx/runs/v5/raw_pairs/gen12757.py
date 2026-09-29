from pyspark.sql import functions as F

result = (
    df_inventory
    .rollup("product_id", F.month("last_restock").alias("last_restock_month"))
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.max("last_restock").alias("latest_last_restock"),
        F.countDistinct("quantity").alias("unique_products"),
    )
)
