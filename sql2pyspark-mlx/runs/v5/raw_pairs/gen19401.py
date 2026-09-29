from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.id"), F.col("o.status"))
    .agg(
        F.count(F.col("p.id")).alias("payment_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("p.status")))).alias("status_values"),
    )
    .orderBy(F.col("status").desc())
)
