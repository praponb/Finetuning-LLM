from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("amount").alias("min_order_amount"))

filtered = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-05-06").cast("date")) & F.col("status").isNotNull())
    .withColumn("opened_date_month_end", F.last_day("opened_date"))
    .withColumn("is_closed", F.when(F.col("status") == "CLOSED", 1).otherwise(0))
)

eligible = (
    filtered
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") >= F.col("s.opened_date")), "left_anti")
)

with_last = (
    eligible
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.account_type",
        "s.opened_date",
        "s.is_closed",
        F.col("c.amount"),
        F.col("c.txn_time"),
    )
)

with_latest = (
    with_last
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.merchant"),
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

result = (
    with_latest
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "amount",
        "txn_time",
        "merchant",
        "transaction_txn_type",
        F.col("min_order_amount"),
    )
    .orderBy(F.col("min_order_amount").desc())
)
