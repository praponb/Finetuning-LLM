from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") > 210) & F.col("quantity").between(3, 7) & F.col("order_id").isin([7, 18, 25]))
    .groupBy(
        F.when(F.col("unit_price") >= 340, "high").when(F.col("unit_price") >= 130, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"))
    .orderBy(F.col("unit_price_bucket").desc(), "stddev_unit_price")
)
