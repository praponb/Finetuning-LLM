from pyspark.sql import functions as F

by_dimension = (
    df_payments
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.channel").alias("channel"), F.col("e.method").alias("method"))
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("e.amount")).alias("total_amount"),
    )
)

result = (
    by_dimension
    .select("channel", "method", "cnt", "total_amount")
    .orderBy("method", "channel")
)
