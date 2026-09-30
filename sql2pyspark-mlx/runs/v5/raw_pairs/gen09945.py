from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("amount").alias("max_order_amount"))

base = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-03-20").cast("date")) & (F.col("opened_date") <= "2023-09-01"))
    .select("*", F.substring("status", 1, 2).alias("status_prefix"))
)

with_dims = (
    base
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .filter(F.col("d.segment") == "Consumer")
    .select("s.*", F.col("d.city"))
)

matched = (
    with_dims
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & F.col("a.opened_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "inner")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

with_latest = (
    matched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.balance",
        "s.opened_date",
        "s.status",
        "s.status_prefix",
        "s.city",
        "s.subscription_id",
        "s.plan",
        "s.monthly_fee",
        F.col("c.merchant"),
    )
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.merchant").isin(["Starbucks", "Amazon"])), "left_semi")
)

valid = (
    not_flagged
    .filter(F.col("opened_date") > F.to_date(F.lit("2023-04-04")))
    .select(
        "*",
        F.initcap(F.trim("plan")).alias("plan_title"),
        F.round("monthly_fee", 0).alias("monthly_fee_rounded"),
    )
)

result = (
    valid
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        "status_prefix",
        "city",
        "subscription_id",
        "plan",
        F.col("max_order_amount"),
    )
    .orderBy(F.col("subscription_id").desc())
)
