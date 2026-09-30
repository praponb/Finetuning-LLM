from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("method", "order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id", "status").orderBy("amount", "id")
w3 = Window.orderBy(F.col("amount_bucket").desc_nulls_last())
w4 = Window.partitionBy("method", "order_id")

base = (
    df_payments
    .filter(F.col("paid_at") >= F.to_timestamp(F.lit("2023-09-27 03:00:00")))
    .withColumn("status_lower", F.lower("status"))
    .withColumn("method_filled", F.coalesce("method", F.lit("N/A")))
)

with_windows = (
    base
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "method_filled",
        F.max("amount").over(w_hist).alias("running_max_amount"),
        F.avg("amount").over(w_hist).alias("running_avg_amount"),
        F.min("amount").over(w_hist).alias("running_min_amount"),
        F.sum("amount").over(w_hist).alias("running_sum_amount"),
        F.ntile(3).over(w2).alias("amount_bucket"),
    )
)

kept_keys = (
    with_windows
    .select("id", "method")
    .exceptAll(
        df_payments
        .filter(F.to_date("paid_at") < "2024-02-01")
        .select("id", "method")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.method") == F.col("k.method")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "running_max_amount",
        "running_min_amount",
        F.dense_rank().over(w3).alias("amount_bucket_dense_rank"),
        F.avg("amount_bucket").over(w4).alias("method_avg_amount_bucket"),
    )
)

result = (
    metrics
    .groupingSets([["order_id", "method"], ["order_id"], []], "order_id", "method")
    .agg(
        F.percentile_approx("running_max_amount", [0.25, 0.5, 0.75]).alias("running_max_amount_quartiles"),
        F.round(F.sum("method_avg_amount_bucket") / F.nullif(F.count(F.when(F.col("method") == "cash", 1)), F.lit(0)), 2).alias("method_avg_amount_bucket_per_cash"),
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
    )
    .filter(F.col("amount_p50") > 0)
    .orderBy(F.col("distinct_order_ids").desc_nulls_last())
)
