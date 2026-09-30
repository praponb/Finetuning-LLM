from pyspark.sql import functions as F

src = (
    df_order_items
    .filter(F.col("unit_price") >= 310)
    .withColumn("unit_price_int", F.col("unit_price").cast("int"))
)

order_id_summary = (
    src
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    order_id_summary
    .filter(F.col("stddev_quantity") < 54)
    .select("order_id", "stddev_quantity")
    .orderBy(F.col("order_id").desc(), F.col("stddev_quantity").desc())
)
