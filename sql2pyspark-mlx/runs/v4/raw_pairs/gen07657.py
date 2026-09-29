from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") == 191)
    .groupBy("product_id")
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("quantity_count") > 8)
    .select("product_id", "min_last_restock", "quantity_count")
    .orderBy(F.col("min_last_restock").desc(), F.col("product_id").desc())
)
