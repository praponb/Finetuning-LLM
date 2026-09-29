from pyspark.sql import functions as F

valid = (
    df_payments
    .filter((~F.col("method").eqNullSafe(F.lit("cash"))) & F.col("status").isin(["SUCCESS", "REFUNDED", "FAILED"]))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        F.initcap(F.trim("status")).alias("status_title"),
        F.concat_ws("-", "status", "method").alias("status_method_key"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .groupingSets(
        [["order_status", "method"], ["order_status"], []],
        F.col("d.status").alias("order_status"),
        F.col("s.method").alias("method"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("s.amount")), 2).alias("avg_amount"),
    )
)
