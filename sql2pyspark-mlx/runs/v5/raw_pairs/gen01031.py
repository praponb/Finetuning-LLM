from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("status").orderBy(F.col("balance").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy(F.col("balance").asc_nulls_last())
w5 = Window.partitionBy("transaction_merchant", "status").orderBy(F.col("balance").desc(), "id")
w6 = Window.partitionBy("transaction_merchant").orderBy("opened_date_plus_months", "id").rowsBetween(-6, -1)
w7 = Window.partitionBy("customer_id", "transaction_merchant").orderBy(F.col("balance_rank").desc_nulls_last(), "id")
shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("shipping_fee").alias("max_order_shipping_fee"))

cleaned = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.col("status").eqNullSafe(F.lit("CLOSED"))))
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        F.add_months("opened_date", 6).alias("opened_date_plus_months"),
        F.substring("status", 1, 2).alias("status_prefix"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.merchant").alias("transaction_merchant"))
)

enriched = (
    with_latest
    .withColumn("balance_bucket", F.ntile(3).over(w2))
    .withColumn("cumulative_balance", F.sum("balance").over(w3))
    .withColumn("balance_rank", F.rank().over(w4))
    .withColumn("balance_bucket_2", F.ntile(10).over(w5))
    .withColumn("balance_avg_6_prior", F.avg("balance").over(w6))
)

latest = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
)

not_flagged = (
    latest
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.amount") >= 1500), "left_semi")
)

result = (
    not_flagged
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date_plus_months",
        "status_prefix",
        "transaction_merchant",
        "balance_bucket",
        "cumulative_balance",
        "balance_bucket_2",
        F.col("max_order_shipping_fee"),
    )
)
