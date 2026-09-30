from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("account_type").orderBy("opened_date", "id")
w_recent_2 = Window.partitionBy("status").orderBy(F.col("status_length").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy("prev_status_length", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("next_balance", "id", "subscription_id").rowsBetween(-6, -1)
w5 = Window.partitionBy("plan", "status").orderBy("status_length", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status", "plan").orderBy(F.col("subscription_id").asc_nulls_first(), "id", "subscription_id")
w_grp = Window.partitionBy("status").orderBy("seq_no", "id", "subscription_id").rowsBetween(-6, Window.currentRow)

filtered = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.col("balance") > 24000))
    .withColumn("clean_account_type", F.trim("account_type"))
    .withColumn("status_length", F.length("status"))
    .withColumn("status_clean", F.coalesce("status", F.lit("DIRECT")))
)

with_windows = (
    filtered
    .filter(F.col("status_length").between(36, 53))
    .withColumn("prev_status_length", F.lag("status_length").over(w_recent))
    .withColumn("prev_balance", F.lag("balance").over(w_recent))
    .withColumn("next_balance", F.lead("balance").over(w_recent))
    .withColumn("seq_no", F.row_number().over(w_recent))
    .withColumn("status_length_bucket", F.ntile(3).over(w_recent_2))
)

matched = (
    with_windows
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.opened_date") >= F.col("sub.start_date")) & (F.col("s.opened_date") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

scored = (
    matched
    .select(
        "id",
        "balance",
        "status",
        "status_clean",
        "next_balance",
        "seq_no",
        "subscription_id",
        "plan",
        F.sum("seq_no").over(w3).alias("cumulative_seq_no"),
        F.avg("balance").over(w_ordered).alias("balance_avg_6_prior"),
        F.last("subscription_id", ignorenulls=True).over(w5).alias("last_known_subscription_id"),
    )
)

deduped = (
    scored
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

enriched = (
    deduped
    .select(
        "*",
        F.sum("last_known_subscription_id").over(w_grp).alias("last7_sum_last_known_subscription_id"),
        F.min("last_known_subscription_id").over(w_grp).alias("last7_min_last_known_subscription_id"),
        F.avg("last_known_subscription_id").over(w_grp).alias("last7_avg_last_known_subscription_id"),
    )
)

result = (
    enriched
    .select(
        "id",
        "subscription_id",
        "status",
        "last7_avg_last_known_subscription_id",
        "balance",
    )
    .unpivot(
        ids=["id", "subscription_id", "status"],
        values=["last7_avg_last_known_subscription_id", "balance"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
