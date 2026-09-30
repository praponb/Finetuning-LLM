from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("account_type")
w3 = Window.partitionBy("customer_id", "status").orderBy(F.col("amount").asc_nulls_last())
w4 = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy("customer_id").orderBy("opened_date", "id")
w6 = Window.partitionBy("status", "account_type").orderBy(F.col("event_seq").asc_nulls_last())
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("discount").alias("min_order_discount"))

with_latest = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount"))
)

metrics = (
    with_latest
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w2)), 4))
    .withColumn("account_type_min_balance", F.min("balance").over(w2))
    .withColumn("amount_cume_dist", F.cume_dist().over(w3))
    .withColumn("amount_avg_2_prior", F.avg("amount").over(w4))
    .withColumn("event_seq", F.row_number().over(w5))
)

enriched = (
    metrics
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.email"))
)

kept = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.hour(F.col("c.txn_time")).between(10, 18)), "left_anti")
)

enriched_accounts = (
    kept
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .select(
        "e.*",
        F.col("p.signup_date"),
        F.col("p.email").alias("customer_email"),
        F.col("p.name"),
    )
)

ranked = (
    enriched_accounts
    .withColumn("latest_rank", F.rank().over(w6))
    .filter(F.col("latest_rank") == 1)
)

latest_match = (
    ranked
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.merchant").alias("transaction_merchant"))
)

result = (
    latest_match
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "account_type_min_balance",
        "amount_cume_dist",
        "amount_avg_2_prior",
        F.when(F.col("status").rlike("^(CLOSED|FROZEN)$"), "CORE").when(F.col("status").rlike("^(OPEN)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("status_zone"),
        F.col("min_order_discount"),
    )
    .orderBy(F.col("status_zone").asc_nulls_first())
)
