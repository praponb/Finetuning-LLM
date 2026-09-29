from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 2, 5, 7]) & F.col("product_id").isin([1, 8, 9, 10]))
    .groupBy(
        "store_id",
        "product_id",
        F.month("last_restock").alias("last_restock_month"),
    )
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.min("quantity").alias("min_quantity"),
    )
    .orderBy("last_restock_month")
)
