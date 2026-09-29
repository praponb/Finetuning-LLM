from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "plan").orderBy("balance", "id", "subscription_id")
w_part = Window.partitionBy("plan").orderBy("subscription_id", "id", "subscription_id")

cleaned = (
    df_accounts
    .filter(F.col("opened_date") > F.lit("2023-06-19").cast("date"))
    .withColumn("status_account_type", F.concat("status", F.lit(", "), "account_type"))
)

with_window = (
    cleaned
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.opened_date") >= F.col("sub.start_date")) & (F.col("s.opened_date") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

valid = (
    with_window
    .filter(F.col("customer_id").isin([31, 40]))
    .select("*", F.substring("account_type", 1, 3).alias("account_type_prefix"))
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "balance",
        "status",
        "subscription_id",
        "plan",
        "monthly_fee",
        "account_type_prefix",
    )
)

not_flagged = (
    latest
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.account_id").isin([1, 20, 30])), "left_anti")
)

valid_accounts = (
    not_flagged
    .filter(F.col("plan") == "premium")
    .select(
        "id",
        "status",
        "subscription_id",
        "plan",
        "monthly_fee",
        "account_type_prefix",
        F.lower("status").alias("status_lower"),
    )
)

scored = (
    valid_accounts
    .filter(F.col("plan").isin(["premium", "standard", "basic"]))
    .select(
        "*",
        F.lag("monthly_fee").over(w_part).alias("prev_monthly_fee"),
        F.lead("subscription_id").over(w_part).alias("next_subscription_id"),
        F.lag("subscription_id").over(w_part).alias("prev_subscription_id"),
        F.row_number().over(w_part).alias("seq_no"),
    )
)

result = (
    scored
    .groupBy(
        "seq_no",
        "prev_subscription_id",
        F.when(F.col("subscription_id") >= 11, "PREMIUM").otherwise("STANDARD").alias("subscription_id_tier"),
    )
    .agg(
        F.percentile_approx("prev_monthly_fee", 0.9).alias("prev_monthly_fee_p90"),
        F.max(F.when(F.col("status") == "OPEN", F.col("seq_no"))).alias("open_max_seq_no"),
    )
    .orderBy(F.col("open_max_seq_no").desc())
)
