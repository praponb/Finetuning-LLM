from pyspark.sql import functions as F

valid = (
    df_payments
    .filter((F.col("paid_at") >= F.to_timestamp(F.lit("2023-05-10 06:00:00"))) & F.col("order_id").isNotNull())
    .withColumn("status_method_key", F.concat_ws("-", "status", "method"))
    .withColumn("amount_adjusted", F.round((F.col("amount") * 0.85), 2))
)

result = (
    valid
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["order_status", "payment_status"], ["order_status"], []],
        F.col("d.status").alias("order_status"),
        F.col("src.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("src.amount")).alias("max_amount"),
        F.sum(F.col("src.amount_adjusted")).alias("total_amount_adjusted"),
    )
    .orderBy(F.col("max_amount").desc())
)
