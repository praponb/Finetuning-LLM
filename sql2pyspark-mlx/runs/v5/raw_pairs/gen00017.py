from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("is_savings", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

cleaned = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "status",
        F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0).alias("is_savings"),
        F.coalesce("status", F.lit("N/A")).alias("status_filled"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.max("is_savings").over(w).alias("last7_max_is_savings"),
        F.count("is_savings").over(w).alias("last7_count_is_savings"),
        F.min("is_savings").over(w).alias("last7_min_is_savings"),
        F.avg("is_savings").over(w).alias("last7_avg_is_savings"),
        F.avg("is_savings").over(w2).alias("is_savings_avg_3"),
    )
)

eligible = (
    scored
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") <= F.col("s.opened_date")), "left_anti")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.account_type",
        "s.status",
        "s.last7_max_is_savings",
        "s.last7_count_is_savings",
        F.col("c.merchant"),
    )
)

with_last = (
    latest_match
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.merchant").alias("transaction_merchant"))
)

result = (
    with_last
    .groupBy("status")
    .agg(
        F.max(F.when(F.col("transaction_merchant") == "7-Eleven", F.col("last7_max_is_savings"))).alias("7_eleven_max_last7_max_is_savings"),
        F.sum(F.when(F.col("status") == "OPEN", F.col("last7_max_is_savings")).otherwise(0)).alias("open_last7_max_is_savings"),
        F.round(F.avg("last7_max_is_savings"), 2).alias("avg_last7_max_is_savings"),
        F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_rows"),
        F.sum("last7_count_is_savings").alias("total_last7_count_is_savings"),
        F.size(F.collect_set("transaction_merchant")).alias("num_transaction_merchants"),
    )
)
