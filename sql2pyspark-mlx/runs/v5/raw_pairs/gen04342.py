from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w2 = Window.orderBy(F.col("subscription_id").asc_nulls_first())
w3 = Window.orderBy("subscription_id", "id", "subscription_id")
w4 = Window.orderBy("balance", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("account_type").orderBy("subscription_id_percent_rank", "id", "subscription_id")

cleaned = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "OPEN"]))
    .withColumn("opened_date_minus_46d", F.date_sub("opened_date", 46))
)

with_window = (
    cleaned
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.opened_date_minus_46d") >= F.col("sub.start_date")) & (F.col("e.opened_date_minus_46d") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

cleaned_accounts = (
    with_window
    .filter(F.col("opened_date_minus_46d").between("2024-03-29", "2025-03-12"))
    .select(
        "*",
        F.date_trunc("YEAR", "opened_date_minus_46d").alias("opened_date_minus_46d_period"),
    )
)

with_last = (
    cleaned_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.account_type",
        "s.balance",
        "s.status",
        "s.opened_date_minus_46d",
        "s.subscription_id",
        "s.plan",
        "s.opened_date_minus_46d_period",
        F.col("c.txn_time"),
    )
)

scored = (
    with_last
    .filter(F.col("balance").between(20000, 51000))
    .select(
        "*",
        F.percent_rank().over(w2).alias("subscription_id_percent_rank"),
        F.ntile(10).over(w3).alias("subscription_id_bucket"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "status",
        "opened_date_minus_46d",
        "subscription_id",
        "plan",
        "opened_date_minus_46d_period",
        "txn_time",
        F.first("status", ignorenulls=True).over(w4).alias("first_known_status"),
        F.lead("subscription_id", 1, 0).over(w_hist).alias("next_subscription_id"),
    )
)

remaining_keys = (
    enriched
    .select("id", "status")
    .intersectAll(
        df_accounts
        .filter(F.col("customer_id") == 10)
        .select("id", "status")
    )
)

verified = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("year_start", F.date_trunc("YEAR", "txn_time"))
    .withColumn("week_start", F.date_trunc("WEEK", "opened_date_minus_46d_period"))
    .groupingSets(
        [["year_start", "plan", "week_start"], ["year_start", "plan"]],
        "year_start",
        "plan",
        "week_start",
    )
    .agg(
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.round(F.avg("subscription_id"), 2).alias("avg_subscription_id"),
        F.min("subscription_id").alias("min_subscription_id"),
        F.round(F.sum("subscription_id") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("subscription_id_per_enterprise"),
        F.max("subscription_id").alias("max_subscription_id"),
        F.round(F.avg("next_subscription_id"), 2).alias("avg_next_subscription_id"),
        F.grouping_id().alias("grouping_id"),
    )
)
