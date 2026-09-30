from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("customer_id").orderBy("opened_date_plus_months", "id")
w_part = Window.partitionBy("account_type").orderBy("opened_date", "id")
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("amount").alias("max_order_amount"))

valid = (
    df_accounts
    .filter(F.col("opened_date") >= F.to_date(F.lit("2023-09-09")))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.add_months("opened_date", 1).alias("opened_date_plus_months"),
        F.col("balance").cast("int").alias("balance_int"),
    )
)

with_last = (
    valid
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time").alias("transaction_txn_time"))
)

ranked = (
    with_last
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

with_windows = (
    ranked
    .select(
        "id",
        "customer_id",
        "opened_date_plus_months",
        "balance_int",
        F.lead("balance").over(w_part).alias("next_balance"),
        F.lead("balance_int").over(w_part).alias("next_balance_int"),
        F.row_number().over(w_part).alias("row_seq"),
    )
)

attributed = (
    with_windows
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.opened_date_plus_months") >= F.col("sub.start_date")) & (F.col("e.opened_date_plus_months") <= F.date_add(F.col("sub.end_date"), 7)), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    attributed
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "opened_date_plus_months",
        "next_balance",
        "next_balance_int",
        "row_seq",
        "subscription_id",
        "plan",
        F.when(F.col("plan").rlike("^(enterprise|standard)$"), "GROUP_B").when(F.col("plan").rlike("^(basic|premium)$"), "EXTENDED").otherwise("GROUP_A").alias("plan_zone"),
        F.col("max_order_amount"),
    )
    .orderBy(F.col("next_balance_int").asc_nulls_first())
)
