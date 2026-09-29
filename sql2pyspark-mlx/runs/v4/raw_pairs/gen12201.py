from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant")
w2 = Window.partitionBy("account_id", "txn_type").orderBy(F.col("txn_time").desc(), "id")
w3 = Window.partitionBy("account_status").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("txn_time", "id")
w5 = Window.partitionBy("merchant", "status")
w6 = Window.partitionBy("account_status", "account_id").orderBy("amount", "id")

metrics = (
    df_transactions
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w)), 4))
    .withColumn("row_seq", F.row_number().over(w2))
)

with_dims = (
    metrics
    .alias("e")
    .join(df_accounts.alias("p"), F.col("e.account_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.status"), F.col("p.balance"))
)

with_dims_transactions = (
    with_dims
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "left")
    .select(
        "s.*",
        F.col("a.opened_date"),
        F.col("a.account_type"),
        F.col("a.status").alias("account_status"),
    )
)

enriched = (
    with_dims_transactions
    .withColumn("cumulative_amount", F.sum("amount").over(w3))
    .withColumn("prev_amount_share", F.lag("amount_share", 2).over(w4))
    .withColumn("merchant_count_amount_share", F.count("amount_share").over(w5))
    .withColumn("seq_no", F.row_number().over(w6))
)

result = (
    enriched
    .cube(F.date_trunc("YEAR", "txn_time").alias("year_start"))
    .agg(
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"),
        F.sum(F.when(F.col("merchant") == "Starbucks", F.col("row_seq")).otherwise(0)).alias("starbucks_row_seq"),
        F.count_if(F.col("merchant") == "7-Eleven").alias("7_eleven_events"),
        F.percentile_approx("cumulative_amount", [0.25, 0.5, 0.75]).alias("cumulative_amount_quartiles"),
        F.max("txn_time").alias("ended_at"),
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_rows"),
    )
    .orderBy(F.col("frozen_count").desc_nulls_last())
)
