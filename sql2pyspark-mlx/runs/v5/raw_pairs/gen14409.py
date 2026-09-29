from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("plan").orderBy("subscription_id", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("plan").orderBy("running_count_monthly_fee", "id", "subscription_id")
w_grp = Window.partitionBy("account_type")
w4 = Window.partitionBy("plan")

cleaned = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-06-20").cast("date")) & F.col("status").isNotNull())
    .withColumn("abs_balance", F.abs("balance"))
)

attributed = (
    cleaned
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.opened_date") >= F.col("sub.start_date")) & (F.col("a.opened_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

metrics = (
    attributed
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "abs_balance",
        "subscription_id",
        "plan",
        "monthly_fee",
        F.count("monthly_fee").over(w_ordered).alias("running_count_monthly_fee"),
        F.sum("monthly_fee").over(w_ordered).alias("running_sum_monthly_fee"),
        F.max("monthly_fee").over(w_ordered).alias("running_max_monthly_fee"),
    )
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") >= F.col("s.opened_date")), "left_anti")
)

enriched = (
    not_flagged
    .filter(F.col("subscription_id") >= 4)
    .select(
        "id",
        "account_type",
        "balance",
        "status",
        "abs_balance",
        "subscription_id",
        "plan",
        "running_sum_monthly_fee",
        F.ntile(5).over(w2).alias("running_count_monthly_fee_bucket"),
        F.count("monthly_fee").over(w_grp).alias("account_type_count_monthly_fee"),
        F.count("running_count_monthly_fee").over(w4).alias("plan_count_running_count_monthly_fee"),
    )
)

grouped = (
    enriched
    .groupBy("account_type")
    .agg(
        F.min("abs_balance").alias("min_abs_balance"),
        F.percentile_approx("plan_count_running_count_monthly_fee", [0.25, 0.5, 0.75]).alias("plan_count_running_count_monthly_fee_quartiles"),
        F.count_if(F.col("plan") == "standard").alias("standard_events"),
    )
)

result = (
    grouped
    .select(
        "account_type",
        "min_abs_balance",
        "standard_events",
        F.when(F.col("account_type").rlike("^(LOAN|CREDIT)$"), "EXTENDED").otherwise("GROUP_B").alias("account_type_zone"),
    )
)
