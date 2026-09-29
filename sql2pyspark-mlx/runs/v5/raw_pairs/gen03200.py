from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 4, 5, 10]))
    .groupBy(F.year("last_restock").alias("last_restock_year"))
    .agg(
        F.count("*").alias("inventory_count"),
        F.sum("quantity").alias("total_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
    .orderBy(F.col("max_quantity").desc())
)
