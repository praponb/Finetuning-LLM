from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w2 = Window.partitionBy("status", "order_id")
w3 = Window.partitionBy("is_success_sum_30d_tier").orderBy(F.col("failed_rows").desc())

base = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-07-05").cast("date")) & F.col("method").isNotNull() & F.col("amount").between(690, 880))
    .select(
        "*",
        F.when(F.col("status") == "SUCCESS", 1).otherwise(0).alias("is_success"),
    )
)

kept_keys = (
    base
    .select("method", "status")
    .subtract(
        df_payments
        .filter(~F.col("method").isin(["promptpay", "paypal"]))
        .select("method", "status")
    )
)

reconciled = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.method") == F.col("k.method")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "*",
        F.sum("is_success").over(w).alias("is_success_sum_30d"),
        F.count("amount").over(w2).alias("status_count_amount"),
    )
)

joined = (
    with_windows
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .select("s.*", F.col("o.order_date"))
)

result = (
    joined
    .groupBy(
        F.when(F.col("is_success_sum_30d") >= 412, "PREMIUM").otherwise("STANDARD").alias("is_success_sum_30d_tier"),
        F.date_trunc("WEEK", "paid_at").alias("week_start"),
        "method",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_rows"),
        F.min("paid_at").alias("first_paid_at"),
    )
    .withColumn("failed_rows_rank", F.rank().over(w3))
    .orderBy(F.col("num_rows").desc_nulls_last())
)
