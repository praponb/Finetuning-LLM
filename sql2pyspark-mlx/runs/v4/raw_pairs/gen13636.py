from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.orderBy("balance", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

base = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-08-10").cast("date")) & F.col("balance").between(70000, 72000))
    .withColumn("balance_whole", F.ceil("balance"))
    .withColumn("balance_rounded", F.round("balance", 1))
    .withColumn("status_filled", F.coalesce("status", F.lit("OTHER")))
)

enriched = (
    base
    .select(
        "*",
        F.avg("balance_whole").over(w).alias("balance_whole_avg_5"),
        F.lag("balance_rounded", 1).over(w2).alias("prev_balance_rounded"),
    )
)

with_latest = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_type"))
)

filtered = (
    with_latest
    .filter(F.col("balance_whole_avg_5") > 48)
    .withColumn("balance_whole_per_prev_balance_rounded", (F.col("balance_whole") / F.nullif("prev_balance_rounded", F.lit(0))))
    .withColumn("resolved_txn_type", F.coalesce("txn_type", F.lit("DIRECT")))
)

summary = (
    filtered
    .groupBy("account_type")
    .agg(
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("balance_whole_avg_5", 0.95).alias("balance_whole_avg_5_p95"),
        F.count_if(F.col("account_type") == "SAVINGS").alias("savings_events"),
    )
)

result = (
    summary
    .select(
        "account_type",
        "num_txn_types",
        "account_type_list",
        "num_rows",
        "savings_events",
    )
    .orderBy(F.col("account_type").asc_nulls_first(), F.col("num_txn_types").desc())
)
