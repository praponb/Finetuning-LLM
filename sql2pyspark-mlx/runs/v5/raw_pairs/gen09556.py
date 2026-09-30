from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type")
w2 = Window.partitionBy("txn_type").orderBy(F.col("amount").asc_nulls_last())
w3 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.partitionBy("account_id", "txn_type").orderBy("amount", "id")
w6 = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(-5, Window.currentRow)
w7 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w8 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w9 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("merchant", "txn_type").orderBy(F.col("prev_txn_time").asc_nulls_last(), "id")

cleaned = (
    df_transactions
    .filter(F.col("txn_time") > F.to_date(F.lit("2023-10-08")))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label_2"),
        F.coalesce("merchant", F.lit("OTHER")).alias("merchant_clean"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.rank().over(w2).alias("amount_rank"),
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share_2"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "is_flagged_label",
        "is_flagged_label_2",
        "merchant_clean",
        "amount_share",
        "amount_share_2",
        F.max("amount_share").over(w3).alias("amount_share_max_90d"),
        F.max("amount_share").over(w4).alias("amount_share_max_30d"),
        F.ntile(5).over(w5).alias("amount_bucket"),
        F.avg("amount_share").over(w6).alias("amount_share_avg_5"),
        F.lag("amount_share", 1).over(w7).alias("prev_amount_share"),
    )
)

prev_events = scored.withColumn("prev_txn_time", F.lag("txn_time").over(w8))

flagged = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_burst").over(w9))

latest = (
    sessionized
    .withColumn("row_num", F.row_number().over(w10))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .groupBy(F.date_trunc("YEAR", "txn_time").alias("year_start"))
    .agg(
        F.avg(F.when(F.col("txn_type") == "FEE", F.col("is_new_burst"))).alias("fee_avg_is_new_burst"),
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("prev_amount_share")).otherwise(0)).alias("refund_prev_amount_share"),
        F.min("session_no").alias("min_session_no"),
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("amount_bucket"))).alias("refund_total_amount_bucket"),
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_count"),
    )
)
