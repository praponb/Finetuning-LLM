from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("account_type").orderBy(F.col("opened_date").desc(), "id")
w3 = Window.partitionBy("account_type").orderBy("opened_date", "id", "subscription_id")
w4 = Window.partitionBy("status", "plan").orderBy(F.col("subscription_id").desc_nulls_first())

valid = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.col("status").eqNullSafe(F.lit("CLOSED"))))
    .withColumn("balance_whole", F.ceil("balance"))
    .withColumn("status_account_type", F.concat("status", F.lit("/"), "account_type"))
    .withColumn("resolved_status", F.coalesce("status", F.lit("DIRECT")))
)

eligible = (
    valid
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") < F.col("s.opened_date")), "left_semi")
)

with_last = (
    eligible
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.account_type",
        "s.balance",
        "s.opened_date",
        "s.status",
        "s.resolved_status",
        F.col("c.amount"),
    )
)

latest = (
    with_last
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_window = (
    latest
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.opened_date") >= F.col("sub.start_date")) & (F.col("e.opened_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

scored = (
    with_window
    .filter(F.col("monthly_fee") < 180)
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "status",
        "resolved_status",
        "subscription_id",
        "plan",
        "monthly_fee",
        F.lead("balance", 1).over(w3).alias("next_balance"),
        F.cume_dist().over(w4).alias("subscription_id_cume_dist"),
    )
)

result = (
    scored
    .groupBy("subscription_id")
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows"),
    )
    .orderBy("subscription_id")
)
