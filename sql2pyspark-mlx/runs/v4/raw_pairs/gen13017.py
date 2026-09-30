from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") < 210)
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
    .orderBy(F.col("avg_unit_price").desc(), "min_unit_price")
)
