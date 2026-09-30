from pyspark.sql import functions as F

cleaned = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2024-04-11").cast("date")) & (F.col("paid_at") < "2024-03-01"))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        F.when(F.col("amount") >= 1230, "High").when(F.col("amount") >= 560, "Medium").otherwise("Low").alias("amount_level"),
        F.coalesce("method", F.lit("OTHER")).alias("method_clean"),
    )
)

result = (
    cleaned
    .groupBy("order_id")
    .pivot("status", ["FAILED", "REFUNDED"])
    .agg(F.avg("amount").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "order_id",
        F.col("FAILED_value_sum").alias("failed_value_sum"),
        F.col("FAILED_cnt").alias("failed_cnt"),
        F.col("REFUNDED_value_sum").alias("refunded_value_sum"),
        F.col("REFUNDED_cnt").alias("refunded_cnt"),
    )
)
