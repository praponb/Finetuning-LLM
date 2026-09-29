from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w_grp = Window.orderBy("amount", "id")
w3 = Window.partitionBy("transaction_txn_type", "transaction_merchant").orderBy("transaction_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("transaction_txn_type").orderBy("amount", "id")
w5 = Window.partitionBy("transaction_merchant")
w6 = Window.partitionBy("transaction_merchant", "transaction_txn_type").orderBy(F.col("amount").desc(), "id")

base = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-03-26").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("status_upper", F.upper("status"))
    .withColumn("account_type_prefix", F.substring("account_type", 1, 4))
    .withColumn("resolved_status", F.coalesce("status", F.lit("NONE")))
)

with_last = (
    base
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.status_upper",
        F.col("c.merchant").alias("transaction_merchant"),
        F.col("c.amount").alias("transaction_amount"),
    )
)

with_latest = (
    with_last
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount"), F.col("c.txn_type").alias("transaction_txn_type"))
)

with_windows = (
    with_latest
    .withColumn("amount_bucket", F.ntile(3).over(w_grp))
    .withColumn("last_known_transaction_txn_type", F.last("transaction_txn_type", ignorenulls=True).over(w3))
    .withColumn("amount_bucket_2", F.ntile(5).over(w4))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w5)), 4))
    .withColumn("event_seq", F.row_number().over(w6))
)

result = (
    with_windows
    .rollup("transaction_merchant")
    .agg(
        F.countDistinct("transaction_txn_type").alias("distinct_transaction_txn_types"),
        F.percentile_approx("amount_bucket", [0.25, 0.5, 0.75]).alias("amount_bucket_quartiles"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.count(F.when(F.col("transaction_merchant") == "7-Eleven", 1)).alias("7_eleven_rows"),
        F.grouping_id().alias("grouping_level"),
    )
)
