from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 190, "high").when(F.col("quantity") >= 114, "mid").otherwise("low").alias("quantity_bucket"),
        "store_id",
        "product_id",
    )
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
    .filter(F.col("avg_quantity") >= 51)
    .orderBy(F.col("store_id").desc())
)
