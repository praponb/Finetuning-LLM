from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id", "merchant").orderBy(F.col("is_new_burst").desc_nulls_last())
w4 = Window.partitionBy("status").orderBy("txn_time", "id")
w5 = Window.partitionBy("status").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("txn_type", "status").orderBy(F.col("prev_txn_time").desc(), "id")
w7 = Window.partitionBy("status").orderBy("session_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("account_id").orderBy(F.col("is_new_burst").desc(), "id")
w9 = Window.orderBy(F.col("session_no").desc(), "id")
w10 = Window.partitionBy("txn_type").orderBy(F.col("prev_txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w11 = Window.orderBy(F.col("amount").desc(), "id")
w12 = Window.partitionBy("account_id").orderBy("prev_txn_time", "id")
w13 = Window.partitionBy("account_id").orderBy("prev_txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_transactions
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

latest = (
    sessionized
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

with_dims = (
    latest
    .alias("src")
    .join(df_accounts.alias("b"), F.col("src.account_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.status"))
)

with_prev = with_dims.withColumn("prev_txn_time", F.lag("txn_time").over(w4))

flagged = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("island_id", F.sum("new_group_flag").over(w5))

ranked = (
    sessions_transactions
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

scored = (
    ranked
    .select(
        "*",
        F.last("status", ignorenulls=True).over(w7).alias("last_known_status"),
        F.ntile(5).over(w8).alias("is_new_burst_bucket"),
        F.ntile(10).over(w9).alias("session_no_bucket"),
        F.avg("new_group_flag").over(w10).alias("new_group_flag_avg_90d"),
        F.ntile(4).over(w11).alias("amount_bucket"),
    )
)

gaps_2 = (
    scored
    .withColumn("is_new_burst_2", F.when(F.col("prev_txn_time") - F.lag("prev_txn_time").over(w12) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = gaps_2.withColumn("session_id", F.sum("is_new_burst_2").over(w13))

result = (
    islands
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        "is_new_burst",
        "session_no",
        "status",
    )
    .orderBy(F.col("txn_time").desc_nulls_first())
)
