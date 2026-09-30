from pyspark.sql import functions as F

src = df_order_items.filter(F.col("quantity") != 5)

summary = (
    src
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    summary
    .filter(F.col("quantity_count") >= 10)
    .select("quantity_bucket", "quantity_count", "avg_quantity")
    .orderBy(F.col("avg_quantity").desc(), F.col("quantity_count").desc())
)
