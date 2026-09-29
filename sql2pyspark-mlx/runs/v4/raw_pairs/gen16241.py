from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("subscription_id").asc_nulls_first(), "id", "subscription_id")
w2 = Window.orderBy("balance", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan").orderBy("monthly_fee", "id", "subscription_id")
w4 = Window.partitionBy("plan", "customer_id").orderBy("balance", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("plan", "account_type").orderBy("opened_date", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("plan")
w7 = Window.partitionBy("year_start").orderBy(F.col("max_balance").desc())

base = (
    df_accounts
    .filter(F.col("opened_date") >= F.to_date(F.lit("2023-10-25")))
    .withColumn("status_account_type", F.concat("status", F.lit(" "), "account_type"))
    .withColumn("account_type_status_key", F.concat_ws("-", "account_type", "status"))
)

with_window = (
    base
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.opened_date") >= F.col("sub.start_date")) & (F.col("e.opened_date") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

latest = (
    with_window
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

base_accounts = (
    latest
    .filter(F.col("opened_date") > F.lit("2023-10-03").cast("date"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status_account_type",
        "account_type_status_key",
        "subscription_id",
        "plan",
        "monthly_fee",
        "dedup_rank",
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("status_clean"),
    )
)

metrics = (
    base_accounts
    .select(
        "*",
        F.sum("monthly_fee").over(w2).alias("cumulative_monthly_fee"),
        F.ntile(10).over(w3).alias("monthly_fee_bucket"),
        F.sum("balance").over(w4).alias("cumulative_balance"),
        F.sum("monthly_fee_int").over(w5).alias("running_monthly_fee_int"),
        F.sum("subscription_id").over(w6).alias("plan_sum_subscription_id"),
    )
)

result = (
    metrics
    .groupBy(F.date_trunc("YEAR", "opened_date").alias("year_start"), "account_type")
    .agg(
        F.max("balance").alias("max_balance"),
        F.sum(F.when(F.col("account_type") == "SAVINGS", F.col("cumulative_balance")).otherwise(0)).alias("savings_cumulative_balance"),
    )
    .withColumn("max_balance_rank", F.rank().over(w7))
)
