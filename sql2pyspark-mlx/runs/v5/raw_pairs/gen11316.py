from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("status").orderBy(F.col("amount").desc())
w4 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w5 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

filtered = (
    df_payments
    .filter((F.col("paid_at") >= F.to_timestamp(F.lit("2023-04-26 04:00:00"))) & F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
    )
)

remaining_keys = (
    filtered
    .select("status", "order_id")
    .subtract(
        df_payments
        .filter(F.col("method").isin(["bank_transfer", "promptpay"]))
        .select("status", "order_id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .withColumn("status_sum_amount", F.sum("amount").over(w))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w2)), 4))
)

metrics = (
    enriched
    .filter(F.col("status") == "REFUNDED")
    .select(
        "id",
        "amount",
        "paid_at",
        "amount_share",
        F.cume_dist().over(w3).alias("amount_cume_dist"),
        F.stddev_samp("amount_share").over(w).alias("status_stddev_samp_amount_share"),
        F.sum("status_sum_amount").over(w4).alias("status_sum_amount_sum_7d"),
        F.avg("amount").over(w5).alias("amount_avg_90d"),
    )
)

report = (
    metrics
    .select(
        "amount",
        "paid_at",
        "amount_share",
        "amount_cume_dist",
        "status_stddev_samp_amount_share",
    )
)

result = (
    report
    .select("amount", "paid_at", "amount_cume_dist", "status_stddev_samp_amount_share")
    .orderBy(F.col("paid_at").desc())
)
