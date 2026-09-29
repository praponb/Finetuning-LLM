from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("order_id", "method")
w3 = Window.partitionBy("status", "method").orderBy("paid_at", "id")
w4 = Window.partitionBy("method", "status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w6 = Window.orderBy("row_seq", "id").rowsBetween(-5, Window.currentRow)
w7 = Window.partitionBy("status").orderBy("paid_at_period", "id")

filtered = (
    df_payments
    .filter(F.col("paid_at") > F.to_date(F.lit("2023-08-09")))
    .withColumn("amount_tier", F.when(F.col("amount") >= 1450, "High").when(F.col("amount") >= 620, "Medium").otherwise("Low"))
    .withColumn("resolved_method", F.coalesce("method", F.lit("N/A")))
)

base = (
    filtered
    .filter(F.col("method").isNotNull())
    .withColumn("paid_at_period", F.date_trunc("WEEK", "paid_at"))
    .withColumn("method_title", F.initcap(F.trim("method")))
    .withColumn("resolved_method_2", F.coalesce("method", F.lit("DIRECT")))
)

metrics = (
    base
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w)), 4))
    .withColumn("order_id_min_amount", F.min("amount").over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("first_known_amount", F.first("amount", ignorenulls=True).over(w4))
)

with_windows = (
    metrics
    .select(
        "*",
        F.count("amount_share").over(w5).alias("amount_share_count_90d"),
        F.avg("order_id_min_amount").over(w6).alias("order_id_min_amount_avg_5"),
        F.row_number().over(w7).alias("row_seq_2"),
    )
)

cleaned = (
    with_windows
    .filter(F.hour("paid_at_period").between(12, 15))
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        "paid_at_period",
        "resolved_method_2",
        "amount_share",
        "order_id_min_amount",
        "amount_share_count_90d",
        "order_id_min_amount_avg_5",
        F.substring("status", 1, 2).alias("status_prefix"),
        (F.col("first_known_amount") - F.col("amount_share")).alias("first_known_amount_minus_amount_share"),
    )
)

summary = (
    cleaned
    .groupBy(
        "status",
        F.when(F.col("order_id_min_amount_avg_5") >= 1527, "LARGE").otherwise("SMALL").alias("order_id_min_amount_avg_5_tier"),
    )
    .agg(
        F.round(F.sum("order_id_min_amount_avg_5") / F.nullif(F.count(F.when(F.col("status") == "FAILED", 1)), F.lit(0)), 2).alias("order_id_min_amount_avg_5_per_failed"),
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_count"),
        F.avg(F.when(F.col("method") == "promptpay", F.col("order_id_min_amount"))).alias("promptpay_avg_order_id_min_amount"),
    )
)

result = (
    summary
    .select(
        "status",
        "order_id_min_amount_avg_5_tier",
        "promptpay_avg_order_id_min_amount",
    )
    .orderBy(F.col("status").desc())
)
