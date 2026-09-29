from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy(F.col("island_id").desc(), "id")
w4 = Window.orderBy("amount", "id")
w5 = Window.partitionBy("txn_type").orderBy(F.col("new_group_flag").desc(), "id")
w6 = Window.partitionBy("account_id").orderBy("island_id", "id")
w7 = Window.partitionBy("merchant").orderBy("txn_time")
w8 = Window.partitionBy("merchant").orderBy(F.col("session_id_rounded").desc_nulls_first(), "id")
w9 = Window.partitionBy("merchant")
w_hist = Window.partitionBy("merchant").orderBy(F.col("is_new_burst").asc_nulls_first())

with_gaps = (
    df_transactions
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

scored = (
    islands
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.ntile(3).over(w3).alias("island_id_bucket"),
        F.lead("new_group_flag", 1).over(w4).alias("next_new_group_flag"),
        F.row_number().over(w5).alias("row_seq"),
        F.ntile(5).over(w6).alias("island_id_bucket_2"),
    )
)

deduped = (
    scored
    .withColumn("rn", F.rank().over(w7))
    .filter(F.col("rn") == 1)
)

flagged = (
    deduped
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w2))

flagged_2 = (
    sessionized
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized_transactions = flagged_2.withColumn("burst_id", F.sum("is_new_burst").over(w2))

cleaned = (
    sessionized_transactions
    .filter(F.col("merchant").isNotNull())
    .select("*", F.round("session_id", 0).alias("session_id_rounded"))
)

latest = (
    cleaned
    .withColumn("row_num", F.row_number().over(w8))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .withColumn("merchant_avg_next_new_group_flag", F.avg("next_new_group_flag").over(w9))
    .withColumn("is_new_burst_dense_rank", F.dense_rank().over(w_hist))
)
