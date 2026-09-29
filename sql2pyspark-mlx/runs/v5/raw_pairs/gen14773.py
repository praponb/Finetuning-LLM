from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") == 151) & F.col("product_id").isin([3, 4]) & F.col("product_id").isin([3, 7, 10]))
    .groupBy(
        F.when(F.col("quantity") >= 191, "high").when(F.col("quantity") >= 109, "mid").otherwise("low").alias("quantity_bucket"),
        "store_id",
    )
    .agg(F.min("last_restock").alias("first_last_restock"))
    .orderBy(F.col("first_last_restock").desc())
)
