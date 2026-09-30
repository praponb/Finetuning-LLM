from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc_nulls_first())
w3 = Window.orderBy(F.col("amount").desc_nulls_first())
w4 = Window.partitionBy("merchant").orderBy(F.col("amount_whole").asc_nulls_last())
w5 = Window.partitionBy("account_id", "txn_type").orderBy(F.col("amount").desc(), "id")
w6 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w7 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w9 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("account_id").orderBy("burst_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("txn_type").orderBy("burst_id_2", "id").rowsBetween(-5, -1)
w12 = Window.partitionBy("txn_type").orderBy("amount_rank", "id")

valid = (
    df_transactions
    .filter(F.col("txn_time") >= F.lit("2023-09-12 17:00:00").cast("timestamp"))
    .withColumn("amount_whole", F.floor("amount"))
    .withColumn("resolved_merchant", F.coalesce("merchant", F.lit("DIRECT")))
)

enriched = (
    valid
    .withColumn("first_known_amount", F.first("amount", ignorenulls=True).over(w))
    .withColumn("amount_rank", F.rank().over(w2))
    .withColumn("amount_dense_rank", F.dense_rank().over(w3))
    .withColumn("amount_whole_percent_rank", F.percent_rank().over(w4))
    .withColumn("amount_bucket", F.ntile(3).over(w5))
)

gaps = (
    enriched
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w6).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w6) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("is_new_session").over(w7))

gaps_2 = (
    islands
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w8) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands_transactions = gaps_2.withColumn("burst_id_2", F.sum("new_group_flag").over(w9))

metrics = (
    islands_transactions
    .select(
        "*",
        F.sum("burst_id").over(w10).alias("cumulative_burst_id"),
        F.avg("amount_dense_rank").over(w11).alias("amount_dense_rank_avg_5_prior"),
        F.row_number().over(w12).alias("event_seq"),
    )
)

result = (
    metrics
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "is_flagged",
        "amount_whole",
        "resolved_merchant",
        "amount_rank",
        "amount_bucket",
        F.when(F.col("txn_type").rlike("^(CREDIT|REFUND)$"), "GROUP_A").otherwise("PRIORITY").alias("txn_type_zone"),
    )
    .orderBy(F.col("txn_type").desc(), F.col("id").desc())
)
