from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status")
w2 = Window.partitionBy("status")
w3 = Window.partitionBy("plan", "customer_id").orderBy("subscription_id", "id", "subscription_id")
w4 = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("account_type", "customer_id").orderBy("next_subscription_id", "id", "subscription_id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("customer_id")
w7 = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

valid = (
    df_accounts
    .filter(F.col("opened_date") > F.lit("2023-06-30").cast("date"))
    .select(
        "*",
        F.upper("account_type").alias("account_type_upper"),
        F.coalesce("status", F.lit("OTHER")).alias("resolved_status"),
    )
)

attributed = (
    valid
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.opened_date") >= F.col("sub.start_date")) & (F.col("src.opened_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

scored = (
    attributed
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4))
    .withColumn("status_sum_subscription_id", F.sum("subscription_id").over(w2))
    .withColumn("next_subscription_id", F.lead("subscription_id", 1, 0).over(w3))
)

with_last = (
    scored
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_type"))
)

with_windows = (
    with_last
    .select(
        "id",
        "account_type",
        "balance",
        "subscription_id",
        "monthly_fee",
        "monthly_fee_share",
        F.count("status_sum_subscription_id").over(w5).alias("last4_count_status_sum_subscription_id"),
        F.avg("status_sum_subscription_id").over(w5).alias("last4_avg_status_sum_subscription_id"),
        F.sum("status_sum_subscription_id").over(w5).alias("last4_sum_status_sum_subscription_id"),
        F.max("monthly_fee_share").over(w6).alias("customer_id_max_monthly_fee_share"),
    )
)

latest_match = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.merchant").alias("transaction_merchant"),
        F.col("c.amount").alias("transaction_amount"),
    )
)

result = (
    latest_match
    .groupBy("account_type")
    .pivot("transaction_merchant", ["7-Eleven", "Starbucks", "Grab"])
    .agg(F.max("last4_count_status_sum_subscription_id"))
    .select(
        "account_type",
        F.col("7-Eleven").alias("v_7_eleven"),
        F.col("Starbucks").alias("starbucks"),
        F.col("Grab").alias("grab"),
    )
)
