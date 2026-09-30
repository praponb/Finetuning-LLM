from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") > 6)
    .groupBy(
        "order_id",
        F.when(F.col("unit_price") >= 330, "high").when(F.col("unit_price") >= 100, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)
