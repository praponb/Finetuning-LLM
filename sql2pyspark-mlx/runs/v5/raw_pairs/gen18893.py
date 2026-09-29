from pyspark.sql import functions as F

result = (
    df_payments
    .alias("s")
    .join(df_orders.alias("dim"), F.col("s.order_id") == F.col("dim.id"), "inner")
    .cube(
        F.col("dim.status").alias("order_status"),
        F.col("dim.channel").alias("channel"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("s.amount")), 2).alias("avg_amount"),
    )
    .orderBy(F.col("rows_total").desc_nulls_last())
)
