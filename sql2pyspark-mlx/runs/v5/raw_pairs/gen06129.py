from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "order_id",
        "product_id",
        F.when(F.col("unit_price") >= 400, "high").when(F.col("unit_price") >= 190, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"))
)
