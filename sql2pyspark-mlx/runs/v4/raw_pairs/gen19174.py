from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("transaction_txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("customer_id", "status").orderBy(F.col("balance").desc(), "id")
w_running = Window.partitionBy("customer_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w5 = Window.partitionBy("customer_id", "transaction_txn_type").orderBy("txn_time", "id")
w_recent = Window.partitionBy("customer_id").orderBy(F.col("is_open").desc_nulls_first())
w7 = Window.partitionBy("customer_id").orderBy("txn_time", "id")
w8 = Window.partitionBy("customer_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("customer_id").orderBy(F.col("is_open_avg_7d").asc_nulls_last())
w10 = Window.partitionBy("customer_id")
w11 = Window.partitionBy("account_type").orderBy(F.col("island_id").desc(), "id")
w12 = Window.partitionBy("transaction_txn_type").orderBy("is_open", "id")
w13 = Window.partitionBy("transaction_txn_type").orderBy("next_is_open_avg_7d", "id").rowsBetween(-2, -1)
w14 = Window.partitionBy("status", "account_type").orderBy(F.col("is_open").desc(), "id")
w15 = Window.partitionBy("customer_id").orderBy("balance_bucket", "id")

valid = (
    df_accounts
    .filter(F.col("opened_date") > F.lit("2024-04-25").cast("date"))
    .withColumn("is_open", F.when(F.col("status") == "OPEN", 1).otherwise(0))
    .withColumn("resolved_status", F.coalesce("status", F.lit("UNKNOWN")))
)

base = (
    valid
    .filter(F.col("opened_date") >= F.to_date(F.lit("2023-08-14")))
    .select(
        "*",
        F.round((F.col("is_open") * 1.15), 2).alias("adjusted_is_open"),
        F.col("is_open").cast("string").alias("is_open_num"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.txn_time"),
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") <= F.col("s.txn_time")), "left_anti")
)

scored = (
    not_flagged
    .select(
        "*",
        F.avg("balance").over(w2).alias("balance_avg_7d"),
        F.ntile(3).over(w3).alias("balance_bucket"),
        F.avg("is_open").over(w_running).alias("is_open_avg_7d"),
        F.lead("balance", 1).over(w5).alias("next_balance"),
        F.cume_dist().over(w_recent).alias("is_open_cume_dist"),
    )
)

with_gaps = (
    scored
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w7).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w7) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w8))

scored_accounts = (
    sessionized
    .withColumn("is_open_avg_7d_cume_dist", F.cume_dist().over(w9))
    .withColumn("customer_id_stddev_samp_is_open_cume_dist", F.stddev_samp("is_open_cume_dist").over(w10))
    .withColumn("row_seq", F.row_number().over(w11))
    .withColumn("next_is_open_avg_7d", F.lead("is_open_avg_7d", 2).over(w12))
)

result = (
    scored_accounts
    .withColumn("balance_bucket_avg_2_prior", F.avg("balance_bucket").over(w13))
    .withColumn("is_open_bucket", F.ntile(5).over(w14))
    .withColumn("next_next_balance", F.lead("next_balance", 1).over(w15))
)
