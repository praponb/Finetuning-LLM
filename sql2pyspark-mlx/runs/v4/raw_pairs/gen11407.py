from pyspark.sql import functions as F

src = df_order_items.filter(F.col("order_id").isin([10, 11, 20, 35]))

summary = (
    src
    .groupBy("order_id")
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.sum("unit_price").alias("total_unit_price"),
    )
)

result = (
    summary
    .filter(F.col("avg_unit_price") > 310)
    .orderBy("order_id")
)
