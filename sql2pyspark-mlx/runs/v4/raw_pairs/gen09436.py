from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([10, 18, 22, 31]))
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
    )
    .orderBy(F.col("unit_price_range").desc())
)
