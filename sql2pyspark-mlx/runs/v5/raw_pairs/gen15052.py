from pyspark.sql import functions as F

src = (
    df_order_items
    .filter((F.col("quantity") <= 6) & F.col("order_id").isin([4, 12, 21]))
)

agg = (
    src
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.countDistinct("quantity").alias("unique_products"),
    )
)

result = (
    agg
    .filter(F.col("unique_products") > 6)
    .select("quantity_bucket", "stddev_quantity")
    .orderBy("stddev_quantity")
)
