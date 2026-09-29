from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type")
w_recent = Window.partitionBy("account_type").orderBy(F.col("balance").desc())
w3 = Window.orderBy(F.col("balance").desc_nulls_first())
w_ordered = Window.partitionBy("status", "account_type")
w5 = Window.partitionBy("segment")

kept_keys = (
    df_accounts
    .select("status", "id")
    .subtract(
        df_accounts
        .filter(F.col("customer_id") == 17)
        .select("status", "id")
    )
)

verified = (
    df_accounts
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .select(
        "*",
        F.stddev_samp("balance").over(w).alias("account_type_stddev_samp_balance"),
        F.percent_rank().over(w_recent).alias("balance_percent_rank"),
        F.cume_dist().over(w3).alias("balance_cume_dist"),
        F.max("balance").over(w_ordered).alias("status_max_balance"),
        F.max("balance").over(w_ordered).alias("status_max_balance_2"),
    )
)

enriched = (
    with_windows
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.city"), F.col("p.segment"))
)

kept = (
    enriched
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") < F.col("s.opened_date")), "left_semi")
)

metrics = (
    kept
    .select(
        "*",
        F.stddev_samp("status_max_balance").over(w5).alias("segment_stddev_samp_status_max_balance"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("status_max_balance_2") >= 76461, "HIGH").otherwise("LOW").alias("status_max_balance_2_tier"),
    )
    .agg(
        F.round(F.sum("balance_cume_dist") / F.nullif(F.count(F.when(F.col("city") == "New York", 1)), F.lit(0)), 2).alias("balance_cume_dist_per_new_york"),
        F.sum(F.when(F.col("status") == "FROZEN", F.col("status_max_balance_2"))).alias("frozen_total_status_max_balance_2"),
        F.sum(F.when(F.col("account_type") == "LOAN", F.col("segment_stddev_samp_status_max_balance")).otherwise(0)).alias("loan_segment_stddev_samp_status_max_balance"),
        F.percentile_approx("segment_stddev_samp_status_max_balance", [0.25, 0.5, 0.75]).alias("segment_stddev_samp_status_max_balance_quartiles"),
    )
)
