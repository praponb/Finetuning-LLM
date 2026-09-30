from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "customer_id")
w2 = Window.partitionBy("account_type").orderBy("monthly_fee", "id", "subscription_id").rowsBetween(-3, -1)
w3 = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

attributed = (
    df_accounts
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.opened_date") >= F.col("sub.start_date")) & (F.col("src.opened_date") <= F.date_add(F.col("sub.end_date"), 7)), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

scored = (
    attributed
    .withColumn("account_type_avg_balance", F.avg("balance").over(w))
    .withColumn("balance_avg_3_prior", F.avg("balance").over(w2))
)

with_latest = (
    scored
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("transaction_amount"))
)

joined = (
    with_latest
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select("s.*", F.col("c.email"), F.col("c.credit_limit"))
)

cleaned = (
    joined
    .filter((F.col("opened_date") >= F.lit("2023-05-31").cast("date")) & F.col("plan").isNotNull())
    .withColumn("days_since_opened_date", F.datediff(F.current_date(), "opened_date"))
    .withColumn("status_clean", F.coalesce("status", F.lit("UNKNOWN")))
)

cleaned_accounts = (
    cleaned
    .filter(F.col("subscription_id") == 5)
    .select("*", F.date_trunc("WEEK", "opened_date").alias("opened_date_period"))
)

with_dims = (
    cleaned_accounts
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select("a.*", F.col("c.country"), F.col("c.name"), F.col("c.signup_date"))
)

result = (
    with_dims
    .groupBy(
        "plan",
        F.when(F.col("subscription_id") >= 8, "PREMIUM").otherwise("STANDARD").alias("subscription_id_tier"),
        F.date_trunc("WEEK", "opened_date").alias("week_start"),
    )
    .agg(
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.round(F.sum("balance_avg_3_prior") / F.nullif(F.count(F.when(F.col("account_type") == "LOAN", 1)), F.lit(0)), 2).alias("balance_avg_3_prior_per_loan"),
        F.max("opened_date_period").alias("last_seen"),
    )
    .orderBy("week_start")
)
