from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "plan")
w2 = Window.partitionBy("status")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

kept_keys = (
    df_subscriptions
    .select("plan")
    .exceptAll(
        df_subscriptions
        .filter(F.year("end_date") >= 2025)
        .select("plan")
    )
)

verified = (
    df_subscriptions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

kept_keys_2 = (
    verified
    .select("plan", "id")
    .subtract(
        df_subscriptions
        .filter(F.month("start_date").isin([2, 7, 11]))
        .select("plan", "id")
    )
)

kept_rows = (
    verified
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .select(
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.avg("monthly_fee").over(w).alias("customer_id_avg_monthly_fee"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share"),
    )
)

filtered = (
    scored
    .filter((F.col("end_date") >= F.lit("2023-04-24").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "customer_id_avg_monthly_fee",
        "monthly_fee_share",
        (F.col("customer_id_avg_monthly_fee") - F.col("monthly_fee_share")).alias("customer_id_avg_monthly_fee_minus_monthly_fee_share"),
        F.trim("status").alias("clean_status"),
    )
)

result = (
    filtered
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "start_date",
        "end_date",
        "status",
        "customer_id_avg_monthly_fee",
        "monthly_fee_share",
        "customer_id_avg_monthly_fee_minus_monthly_fee_share",
        "clean_status",
        F.col("avg_account_balance"),
    )
    .orderBy(F.col("customer_id").asc_nulls_last(), "end_date")
)
