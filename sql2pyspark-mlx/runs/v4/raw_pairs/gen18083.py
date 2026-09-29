from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 186, "high").when(F.col("quantity") >= 95, "mid").otherwise("low").alias("quantity_bucket"),
        "product_id",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.max("quantity").alias("max_quantity"),
        F.min("last_restock").alias("min_last_restock"),
    )
    .filter(F.col("max_quantity") < 140)
)
