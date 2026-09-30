from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter(F.col("txn_type") != "DEBIT")
    .select(
        "*",
        F.concat("merchant", F.lit(" - "), "txn_type").alias("merchant_txn_type"),
        F.concat("txn_type", F.lit(" - "), "merchant").alias("txn_type_merchant"),
        F.coalesce("merchant", F.lit("OTHER")).alias("resolved_merchant"),
    )
)

enriched = (
    filtered
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "left")
    .select("s.*", F.col("a.status"), F.col("a.opened_date"), F.col("a.account_type"))
)

gaps = (
    enriched
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .rollup(
        F.date_trunc("QUARTER", "opened_date").alias("quarter_start"),
        F.date_trunc("WEEK", "txn_time").alias("week_start"),
    )
    .agg(
        F.percentile_approx("session_id", [0.25, 0.5, 0.75]).alias("session_id_quartiles"),
        F.count_if(F.col("txn_type") == "CREDIT").alias("credit_events"),
        F.count_if(F.col("account_type") == "LOAN").alias("loan_events"),
    )
    .orderBy("loan_events")
)
