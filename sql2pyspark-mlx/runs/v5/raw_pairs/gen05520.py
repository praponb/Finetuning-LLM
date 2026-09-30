from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("method", "order_id")
w3 = Window.partitionBy("method").orderBy(F.col("method_stddev_samp_days_since_paid_at").desc_nulls_last())
w_recent = Window.orderBy(F.col("method_stddev_samp_days_since_paid_at").asc_nulls_last())
w5 = Window.partitionBy("method")
w6 = Window.partitionBy("method").orderBy(F.col("method_stddev_samp_days_since_paid_at").desc_nulls_first())
w7 = Window.partitionBy("method").orderBy(F.col("method_stddev_samp_days_since_paid_at").desc())

deduped = (
    df_payments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "order_id", "method", "paid_at", "status")
)

base = (
    deduped
    .filter(F.col("order_id").isNull())
    .withColumn("method_title", F.initcap(F.trim("method")))
)

cleaned = (
    base
    .filter((F.col("paid_at") > F.lit("2023-05-25 20:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "method_title",
        F.to_date("paid_at").alias("paid_at_date"),
        F.datediff(F.current_date(), "paid_at").alias("days_since_paid_at"),
        F.coalesce("method", F.lit("NONE")).alias("method_clean"),
    )
)

remaining_keys = (
    cleaned
    .select("order_id", "id")
    .intersect(
        df_payments
        .filter(F.col("method") == "paypal")
        .select("order_id", "id")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "method",
        "paid_at_date",
        "method_clean",
        F.stddev_samp("days_since_paid_at").over(w2).alias("method_stddev_samp_days_since_paid_at"),
    )
)

with_windows_payments = (
    with_windows
    .select(
        "method",
        "method_clean",
        F.dense_rank().over(w3).alias("method_stddev_samp_days_since_paid_at_dense_rank"),
        F.percent_rank().over(w_recent).alias("method_stddev_samp_days_since_paid_at_percent_rank"),
        F.stddev_samp("method_stddev_samp_days_since_paid_at").over(w5).alias("method_stddev_samp_method_stddev_samp_days_since_paid_at"),
        F.rank().over(w6).alias("method_stddev_samp_days_since_paid_at_rank"),
        F.rank().over(w7).alias("method_stddev_samp_days_since_paid_at_rank_2"),
    )
)

result = (
    with_windows_payments
    .withColumn("method_stddev_samp_days_since_paid_at_rank_2_tier", F.when(F.col("method_stddev_samp_days_since_paid_at_rank_2") >= 7, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["method", "method_stddev_samp_days_since_paid_at_rank_2_tier"], ["method"], []],
        "method",
        "method_stddev_samp_days_since_paid_at_rank_2_tier",
    )
    .agg(
        F.count(F.when(F.col("method") == "card", 1)).alias("card_rows"),
        F.count_if(F.col("method") == "promptpay").alias("promptpay_events"),
        F.count(F.when(F.col("method") == "promptpay", 1)).alias("promptpay_count"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("method").asc_nulls_last())
)
