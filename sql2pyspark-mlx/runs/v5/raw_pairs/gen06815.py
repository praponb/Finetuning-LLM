from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
    )
)
