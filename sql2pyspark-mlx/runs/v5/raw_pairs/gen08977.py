from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(5, 8))
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
        "product_id",
    )
    .agg(F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"))
    .orderBy(F.col("product_id").desc())
)
