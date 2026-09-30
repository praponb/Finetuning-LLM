from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 6, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
    .filter(F.col("stddev_quantity") < 33)
    .select("product_id", "quantity_bucket", "stddev_quantity")
    .orderBy(F.col("product_id").desc(), "quantity_bucket")
    .limit(10)
)
