from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("monthly_fee", "id", "subscription_id")
w2 = Window.partitionBy("status").orderBy("monthly_fee", "id", "subscription_id")
w3 = Window.partitionBy("status", "account_type").orderBy("monthly_fee", "id", "subscription_id").rowsBetween(-2, -1)
w_hist = Window.partitionBy("account_type").orderBy("subscription_id", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy("seq_no", "id", "subscription_id").rowsBetween(-5, Window.currentRow)
w6 = Window.orderBy("subscription_id", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_accounts
    .filter(F.col("opened_date") >= F.lit("2023-10-01").cast("date"))
    .select(
        "*",
        F.concat_ws(",", "account_type", "status").alias("account_type_status_key"),
        F.coalesce("status", F.lit("NONE")).alias("status_clean"),
    )
)

with_window = (
    base
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.opened_date") >= F.col("sub.start_date")) & (F.col("s.opened_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

metrics = (
    with_window
    .select(
        "id",
        "account_type",
        "status",
        "account_type_status_key",
        "subscription_id",
        "plan",
        "monthly_fee",
        F.lead("balance", 1).over(w).alias("next_balance"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

with_windows = (
    metrics
    .filter(F.col("next_balance") > 61000)
    .withColumn("subscription_id_avg_2_prior", F.avg("subscription_id").over(w3))
    .withColumn("last_known_seq_no", F.last("seq_no", ignorenulls=True).over(w_hist))
    .withColumn("seq_no_avg_5", F.avg("seq_no").over(w5))
    .withColumn("cumulative_next_balance", F.sum("next_balance").over(w6))
)

eligible = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (~F.col("c.is_flagged")), "left_anti")
)

result = (
    eligible
    .groupBy(
        F.when(F.col("cumulative_next_balance") >= 429644, "HIGH").otherwise("LOW").alias("cumulative_next_balance_tier"),
        "status",
        "account_type",
    )
    .agg(
        F.percentile_approx("seq_no", 0.5).alias("seq_no_p50"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_count"),
        F.percentile_approx("last_known_seq_no", [0.25, 0.5, 0.75]).alias("last_known_seq_no_quartiles"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
        F.count("*").alias("cnt"),
    )
    .orderBy("basic_rows")
)
