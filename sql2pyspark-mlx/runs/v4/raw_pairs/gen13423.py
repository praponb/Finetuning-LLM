from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("start_date").desc(), "id")
w_running = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc_nulls_first())
w_running_2 = Window.partitionBy("plan", "customer_id").orderBy("start_date", "id")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.max("balance").alias("max_account_balance"))

metrics = (
    df_subscriptions
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.last("monthly_fee", ignorenulls=True).over(w_running).alias("last_known_monthly_fee"),
        F.dense_rank().over(w_recent).alias("monthly_fee_dense_rank"),
        F.lag("monthly_fee", 1, 0).over(w_running_2).alias("prev_monthly_fee"),
    )
)

attributed = (
    metrics
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.end_date") >= F.col("sub.start_date")) & (F.col("a.end_date") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

kept_keys = (
    attributed
    .select("plan", "id")
    .subtract(
        df_subscriptions
        .filter(F.col("plan") != "basic")
        .select("plan", "id")
    )
)

kept_rows = (
    attributed
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "status",
        "event_seq",
        "last_known_monthly_fee",
        "prev_monthly_fee",
        "subscription_id",
        F.when(F.col("status").rlike("^(PAUSED|ACTIVE)$"), "OTHER_ZONE").otherwise("CORE").alias("status_zone"),
        F.col("max_account_balance"),
    )
    .orderBy(F.col("max_account_balance").asc_nulls_last(), "end_date")
)
