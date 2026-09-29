from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.sum("unit_price").alias("total_unit_price"),
        F.min("unit_price").alias("min_unit_price"),
    )
    .filter(F.col("min_unit_price") > 370)
    .orderBy(F.col("total_unit_price").desc(), "order_id")
    .limit(20)
)
