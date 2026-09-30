from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([3, 5]) | (F.col("store_id") == 3))
    .groupBy(F.year("last_restock").alias("last_restock_year"))
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("max_quantity") > 90)
    .orderBy("avg_quantity", F.col("last_restock_year").desc())
)
