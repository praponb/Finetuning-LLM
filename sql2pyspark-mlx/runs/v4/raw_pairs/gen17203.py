from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("monthly_fee").desc(), "id", "subscription_id")
w2 = Window.partitionBy("customer_id", "status").orderBy("opened_date", "id", "subscription_id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("status", "plan").orderBy("opened_date", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id")
w5 = Window.partitionBy("customer_id").orderBy("balance", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

kept = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") < F.col("s.opened_date")), "left_anti")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") < F.col("s.opened_date")), "left_semi")
)

joined = (
    not_flagged
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select("a.*", F.col("c.country"))
)

attributed = (
    joined
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & F.col("s.opened_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

with_windows = (
    attributed
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.avg("subscription_id").over(w2).alias("subscription_id_avg_6"),
        F.first("country", ignorenulls=True).over(w3).alias("first_known_country"),
        F.sum("subscription_id").over(w4).alias("customer_id_sum_subscription_id"),
        F.last("country", ignorenulls=True).over(w5).alias("last_known_country"),
    )
)

remaining_keys = (
    with_windows
    .select("id", "status")
    .subtract(
        df_accounts
        .filter(F.col("account_type") == "CREDIT")
        .select("id", "status")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter((F.col("opened_date") > F.lit("2023-12-04").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "customer_id",
        "account_type",
        "balance",
        "status",
        "subscription_id",
        "plan",
        "monthly_fee",
        "row_seq",
        "subscription_id_avg_6",
        "customer_id_sum_subscription_id",
        "last_known_country",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("status_clean"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("row_seq") >= 7, "LARGE").otherwise("SMALL").alias("row_seq_tier"),
        "customer_id_sum_subscription_id",
    )
    .agg(
        F.percentile_approx("days_since_opened_date", 0.9).alias("days_since_opened_date_p90"),
        F.count_if(F.col("account_type") == "LOAN").alias("loan_events"),
        F.count_if(F.col("status") == "FROZEN").alias("frozen_events"),
    )
)
