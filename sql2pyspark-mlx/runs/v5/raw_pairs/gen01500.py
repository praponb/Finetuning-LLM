from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
        "order_id",
    )
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
    .filter(F.col("stddev_quantity") >= 72)
    .orderBy("stddev_quantity")
)
