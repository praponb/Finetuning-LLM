from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") >= 68)
    .cube(
        F.when(F.col("quantity") >= 238, "high").when(F.col("quantity") >= 114, "mid").otherwise("low").alias("quantity_bucket"),
        "store_id",
    )
    .agg(
        F.count("*").alias("inventory_count"),
        F.min("quantity").alias("min_quantity"),
    )
    .orderBy(F.col("store_id").desc())
)
