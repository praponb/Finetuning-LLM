from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
        "order_id",
    )
    .agg(F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"))
    .filter(F.col("stddev_unit_price") < 78)
)
