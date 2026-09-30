from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date_period", "id")
w2 = Window.partitionBy("status").orderBy("opened_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id")
w4 = Window.orderBy("balance", "id")
w5 = Window.partitionBy("account_type").orderBy(F.col("balance").desc(), "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w7 = Window.orderBy("transaction_amount", "id")
w_grp = Window.orderBy("customer_id_avg_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy(F.col("opened_date_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w10 = Window.orderBy(F.col("opened_date_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w11 = Window.orderBy(F.col("customer_id_avg_balance").desc())
w12 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

filtered = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.date_trunc("week", "opened_date").alias("opened_date_period"),
        F.coalesce("status", F.lit("NONE")).alias("status_clean"),
    )
)

with_dims = (
    filtered
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.age"))
)

flagged = (
    with_dims
    .withColumn("session_start_flag", F.when(F.lag("opened_date_period").over(w).isNull() | (F.col("opened_date_period") > F.lag("opened_date_period").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_accounts = (
    flagged
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "opened_date_period",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

with_windows = (
    sessions_accounts
    .select(
        "id",
        "opened_date",
        "opened_date_period",
        "burst_id",
        F.avg("balance").over(w3).alias("customer_id_avg_balance"),
        F.row_number().over(w4).alias("row_seq"),
        F.ntile(4).over(w5).alias("balance_bucket"),
    )
)

with_latest = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("transaction_amount"))
)

with_windows_accounts = (
    with_latest
    .select(
        "id",
        "opened_date_period",
        "customer_id_avg_balance",
        F.row_number().over(w7).alias("row_seq_2"),
        F.first("transaction_amount", ignorenulls=True).over(w_grp).alias("first_known_transaction_amount"),
        F.sum("balance_bucket").over(w9).alias("balance_bucket_sum_30d"),
        F.count("row_seq").over(w10).alias("row_seq_count_7d"),
        F.dense_rank().over(w11).alias("customer_id_avg_balance_dense_rank"),
    )
)

with_last = (
    with_windows_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date_period")), "left")
    .withColumn("rn", F.row_number().over(w12))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id_avg_balance",
        "s.row_seq_2",
        "s.first_known_transaction_amount",
        F.col("c.amount"),
        F.col("c.txn_time"),
    )
)

result = (
    with_last
    .groupBy(
        F.when(F.col("first_known_transaction_amount") >= 3800, "HIGH").otherwise("LOW").alias("first_known_transaction_amount_tier"),
        F.date_trunc("YEAR", "txn_time").alias("year_start"),
    )
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.sum("customer_id_avg_balance").alias("total_customer_id_avg_balance"),
        F.min("txn_time").alias("started_at"),
        F.round(F.avg("customer_id_avg_balance"), 2).alias("avg_customer_id_avg_balance"),
    )
    .orderBy(F.col("first_known_transaction_amount_tier").desc_nulls_first())
)
