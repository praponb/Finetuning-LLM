from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 8, 9]))
    .groupBy(
        F.when(F.col("quantity") >= 267, "high").when(F.col("quantity") >= 41, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("quantity_count") < 8)
    .select("quantity_bucket", "quantity_count", "max_quantity")
    .orderBy(F.col("max_quantity").desc(), "quantity_bucket")
    .limit(3)
)
