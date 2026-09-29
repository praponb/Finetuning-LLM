from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("status").orderBy("opened_date", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

cleaned = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-05-27").cast("date")) & F.col("status").isNotNull())
    .select("*", F.trim("account_type").alias("clean_account_type"))
)

metrics = (
    cleaned
    .select(
        "*",
        F.lag("balance").over(w_hist).alias("prev_balance"),
        F.row_number().over(w_hist).alias("event_seq"),
    )
)

with_last = (
    metrics
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.merchant").alias("transaction_merchant"),
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

with_last_accounts = (
    with_last
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.prev_balance",
        "s.event_seq",
        "s.transaction_merchant",
        F.col("c.amount"),
        F.col("c.merchant").alias("transaction_merchant_2"),
    )
)

latest_match = (
    with_last_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.prev_balance",
        "s.event_seq",
        F.col("c.merchant"),
    )
)

result = (
    latest_match
    .groupBy(
        F.when(F.col("event_seq") >= 8, "LARGE").otherwise("SMALL").alias("event_seq_tier"),
        "customer_id",
    )
    .agg(
        F.count_if(F.col("merchant") == "7-Eleven").alias("7_eleven_events"),
        F.count(F.when(F.col("merchant") == "7-Eleven", 1)).alias("7_eleven_count"),
        F.percentile_approx("prev_balance", [0.25, 0.5, 0.75]).alias("prev_balance_quartiles"),
    )
    .orderBy(F.col("7_eleven_count").asc_nulls_last())
)
