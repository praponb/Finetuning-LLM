from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("customer_id", "account_type").orderBy(F.col("balance").desc_nulls_first())
w3 = Window.orderBy("balance", "id")
w4 = Window.partitionBy("account_type").orderBy(F.col("row_seq_filled").desc(), "id", "subscription_id")
w5 = Window.partitionBy("account_type").orderBy("subscription_id", "id", "subscription_id")

base = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-09-13").cast("date")) & (F.col("opened_date") <= "2025-07-30"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.concat("status", F.lit(" "), "account_type").alias("status_account_type"),
        F.coalesce("status", F.lit("DIRECT")).alias("status_clean"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.max("balance").over(w).alias("status_max_balance"),
        F.dense_rank().over(w2).alias("balance_dense_rank"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

enriched = (
    metrics
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.name"))
)

matched = (
    enriched
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & F.col("a.opened_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

cleaned = (
    matched
    .filter((F.col("opened_date") >= F.lit("2023-05-04").cast("date")) & (F.col("subscription_id") == 4))
    .select(
        "id",
        "account_type",
        "opened_date",
        "status",
        "status_account_type",
        "status_clean",
        "status_max_balance",
        "balance_dense_rank",
        "row_seq",
        "name",
        "subscription_id",
        "plan",
        "monthly_fee",
        F.round((F.col("balance_dense_rank") * 0.9), 2).alias("balance_dense_rank_scaled"),
        F.coalesce("row_seq", F.lit(0)).alias("row_seq_filled"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("row_seq_filled") > 64)
    .select(
        "id",
        "opened_date",
        "status_clean",
        "status_max_balance",
        "row_seq",
        "name",
        "subscription_id",
        F.row_number().over(w4).alias("seq_no"),
        F.ntile(3).over(w5).alias("subscription_id_bucket"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("status_max_balance") >= 72915, "PREMIUM").otherwise("STANDARD").alias("status_max_balance_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("subscription_id", 0.95).alias("subscription_id_p95"),
        F.percentile_approx("row_seq", 0.5).alias("row_seq_p50"),
    )
    .filter(F.col("subscription_id_p95") > 0)
    .orderBy("distinct_ids")
)
