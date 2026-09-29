from pyspark.sql import functions as F

src = df_order_items.filter(F.col("order_id") == 40)

summary = (
    src
    .groupBy(
        F.when(F.col("unit_price") >= 400, "high").when(F.col("unit_price") >= 110, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
)

result = (
    summary
    .filter(F.col("avg_quantity") < 40)
    .select("unit_price_bucket", "avg_quantity", "avg_unit_price")
)
