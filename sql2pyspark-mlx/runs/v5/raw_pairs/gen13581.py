from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance")
w_grp = Window.partitionBy("customer_id")

attributed = (
    df_accounts
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.opened_date") >= F.col("sub.start_date")) & (F.col("src.opened_date") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

matched_keys = (
    attributed
    .select("account_type", "status")
    .intersect(
        df_accounts
        .filter(F.col("account_type").isin(["CHECKING", "CREDIT", "SAVINGS"]))
        .select("account_type", "status")
    )
)

verified = (
    attributed
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

kept = (
    verified
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") >= F.col("s.opened_date")), "left_anti")
)

enriched = (
    kept
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .select(
        "e.id",
        "e.customer_id",
        "e.balance",
        "e.opened_date",
        "e.plan",
        "e.monthly_fee",
        F.col("p.segment"),
        F.col("p.name"),
    )
)

base = (
    enriched
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-10-03"))) & (~F.col("plan").eqNullSafe(F.lit("premium"))))
    .select(
        "id",
        "customer_id",
        "balance",
        "monthly_fee",
        "name",
        F.greatest("monthly_fee", "balance").alias("greatest_monthly_fee_balance"),
        (F.col("monthly_fee") - F.col("balance")).alias("monthly_fee_minus_balance"),
        F.coalesce("plan", F.lit("UNKNOWN")).alias("resolved_plan"),
    )
)

metrics = (
    base
    .withColumn("balance_percent_rank", F.percent_rank().over(w))
    .withColumn("customer_id_max_greatest_monthly_fee_balance", F.max("greatest_monthly_fee_balance").over(w_grp))
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.account_id").isin([18, 28])), "left_semi")
)

result = (
    not_flagged
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .rollup(F.col("c.city").alias("city"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("e.customer_id_max_greatest_monthly_fee_balance")), 2).alias("total_customer_id_max_greatest_monthly_fee_balance"),
        F.min(F.col("e.monthly_fee")).alias("min_monthly_fee"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy("total_customer_id_max_greatest_monthly_fee_balance")
)
