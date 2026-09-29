from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "customer_id").orderBy("monthly_fee", "id")
w2 = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("segment", "plan").orderBy("monthly_fee", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country").orderBy(F.col("subscription_id").asc_nulls_first())
w5 = Window.orderBy("credit_limit", "id", "subscription_id")
w6 = Window.partitionBy("segment").orderBy(F.col("prev_monthly_fee").desc(), "id", "subscription_id")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

filtered = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        F.trim("plan").alias("clean_plan"),
        F.when(F.col("plan").isin(["standard", "enterprise"]), "Priority").otherwise("Other").alias("plan_group"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "customer_id",
        "start_date",
        "clean_plan",
        F.lag("monthly_fee", 1).over(w).alias("prev_monthly_fee"),
        F.avg("monthly_fee").over(w2).alias("monthly_fee_avg_5_prior"),
    )
)

with_window = (
    with_windows
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.start_date") >= F.col("sub.start_date")) & (F.col("e.start_date") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

with_dims = (
    with_window
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .filter(F.col("d.segment").isin(["Consumer", "SMB"]))
    .select("src.*", F.col("d.country"), F.col("d.email"), F.col("d.credit_limit"))
)

with_dims_subscriptions = (
    with_dims
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.name"), F.col("p.signup_date"))
)

enriched = (
    with_dims_subscriptions
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select(
        "src.*",
        F.col("c.segment"),
        F.col("c.signup_date").alias("customer_signup_date"),
        F.col("c.country").alias("customer_country"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.first("prev_monthly_fee", ignorenulls=True).over(w3).alias("first_known_prev_monthly_fee"),
        F.dense_rank().over(w4).alias("subscription_id_dense_rank"),
        F.lag("monthly_fee_avg_5_prior", 1, 0).over(w5).alias("prev_monthly_fee_avg_5_prior"),
        F.ntile(4).over(w6).alias("prev_monthly_fee_bucket"),
    )
)

result = (
    metrics
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "clean_plan",
        "prev_monthly_fee",
        "monthly_fee_avg_5_prior",
        "subscription_id",
        "plan",
        "monthly_fee",
        "country",
        F.when(F.col("plan").rlike("^(premium|enterprise)$"), "GROUP_B").otherwise("GROUP_A").alias("plan_zone"),
        F.col("avg_account_balance"),
    )
    .orderBy(F.col("customer_id").desc())
)
