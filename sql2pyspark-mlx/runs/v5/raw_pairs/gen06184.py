from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("subscription_monthly_fee", "id", "subscription_id", "subscription_id_2")
w2 = Window.partitionBy("customer_id").orderBy("subscription_id_2", "id", "subscription_id", "subscription_id_2")
w3 = Window.partitionBy("subscription_plan").orderBy("subscription_id", "id", "subscription_id", "subscription_id_2").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_window = (
    df_accounts
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.opened_date") >= F.col("sub.start_date")) & (F.col("e.opened_date") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

attributed = (
    with_window
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & F.col("a.opened_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id_2"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

kept = (
    attributed
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") < F.col("s.opened_date")), "left_anti")
)

eligible = (
    kept
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") <= F.col("s.opened_date")), "left_anti")
)

ranked = (
    eligible
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "customer_id",
        "subscription_id",
        "plan",
        "monthly_fee",
        "subscription_id_2",
        "subscription_plan",
        "subscription_monthly_fee",
    )
)

result = (
    ranked
    .filter(F.col("subscription_id").between(4, 8))
    .select(
        "id",
        "subscription_id",
        "monthly_fee",
        "subscription_id_2",
        "subscription_plan",
        F.lead("subscription_monthly_fee").over(w2).alias("next_subscription_monthly_fee"),
        F.lag("subscription_monthly_fee").over(w2).alias("prev_subscription_monthly_fee"),
        F.lag("subscription_id").over(w2).alias("prev_subscription_id"),
        F.lead("subscription_id").over(w2).alias("next_subscription_id"),
        F.row_number().over(w2).alias("event_seq"),
        F.last("subscription_monthly_fee", ignorenulls=True).over(w3).alias("last_known_subscription_monthly_fee"),
    )
)
