from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 7]))
    .groupBy("store_id", F.year("last_restock").alias("last_restock_year"))
    .agg(
        F.min("last_restock").alias("first_last_restock"),
        F.avg("quantity").alias("avg_quantity"),
        F.countDistinct("quantity").alias("unique_products"),
    )
    .filter(F.col("unique_products") > 5)
    .drop("unique_products")
)
