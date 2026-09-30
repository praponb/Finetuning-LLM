from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.max("balance").alias("max_account_balance"))

not_flagged = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") <= F.col("s.opened_date")), "left_anti")
)

ranked = (
    not_flagged
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

not_flagged_accounts = (
    ranked
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") < F.col("s.opened_date")), "left_anti")
)

attributed = (
    not_flagged_accounts
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & F.col("e.opened_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

wide = (
    attributed
    .groupBy("account_type", "customer_id")
    .pivot("plan", ["enterprise", "standard"])
    .agg(F.sum("balance").alias("total"), F.count(F.lit(1)).alias("cnt"))
)

result = (
    wide
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "account_type",
        "customer_id",
        "enterprise_total",
        "enterprise_cnt",
        "standard_total",
        F.col("max_account_balance"),
    )
)
