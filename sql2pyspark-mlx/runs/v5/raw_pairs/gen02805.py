from pyspark.sql import functions as F

dim_totals = (
    df_payments
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.channel").alias("channel"), F.col("src.method").alias("method"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("src.amount")).alias("max_amount"),
    )
)

result = (
    dim_totals
    .select("channel", "method", "cnt")
    .orderBy(F.col("cnt").asc_nulls_last())
)
