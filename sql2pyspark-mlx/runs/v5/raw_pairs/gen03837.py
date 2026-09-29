from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("customer_id")
w4 = Window.partitionBy("status", "customer_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

scored = (
    df_accounts
    .filter(F.col("balance") > 70000)
    .select(
        "id",
        "customer_id",
        "balance",
        "status",
        F.avg("balance").over(w).alias("balance_avg_3"),
        F.avg("balance").over(w2).alias("balance_avg_3_prior"),
    )
)

eligible = (
    scored
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.amount").between(2200, 3700)), "left_anti")
)

with_windows = (
    eligible
    .select(
        "*",
        F.max("balance_avg_3").over(w3).alias("customer_id_max_balance_avg_3"),
        F.last("status", ignorenulls=True).over(w4).alias("last_known_status"),
    )
)

latest_match = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time"))
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.account_id").isin([36, 38])), "left_semi")
)

stats = (
    not_flagged
    .groupBy("status")
    .agg(
        F.count_if(F.col("status") == "OPEN").alias("open_events"),
        F.avg("balance_avg_3_prior").alias("avg_balance_avg_3_prior"),
        F.min("txn_time").alias("first_seen"),
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"),
    )
)

result = (
    stats
    .select("status", "avg_balance_avg_3_prior", "first_seen", "open_count")
    .orderBy(F.col("avg_balance_avg_3_prior").asc_nulls_last())
)
