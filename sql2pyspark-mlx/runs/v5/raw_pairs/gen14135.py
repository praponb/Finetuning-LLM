from pyspark.sql import functions as F

cleaned = (
    df_payments
    .filter(F.col("status") == "SUCCESS")
    .select(
        "*",
        F.datediff(F.current_date(), "paid_at").alias("days_since_paid_at"),
        F.coalesce("method", F.lit("DIRECT")).alias("resolved_method"),
    )
)

result = (
    cleaned
    .alias("e")
    .join(df_orders.alias("dim"), F.col("e.order_id") == F.col("dim.id"), "inner")
    .rollup(
        F.col("dim.channel").alias("channel"),
        F.col("e.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("e.amount")).alias("total_amount"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("channel").desc_nulls_last())
)
