from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("product_id").isin([23, 25, 39]))
    .withColumn("line_total", (F.col("unit_price") * F.col("quantity")))
)

summary = (
    base
    .groupBy(
        F.when(F.col("line_total") >= 65, "high").when(F.col("line_total") >= 28, "mid").otherwise("low").alias("line_total_bucket"),
    )
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)

result = (
    summary
    .select("line_total_bucket", "stddev_quantity")
    .orderBy("line_total_bucket")
)
