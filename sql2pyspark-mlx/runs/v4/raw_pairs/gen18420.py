from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w4 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").desc_nulls_first(), "id")
w6 = Window.partitionBy("merchant").orderBy(F.col("prev_txn_time").desc(), "id")
w7 = Window.partitionBy("txn_type").orderBy("prev_txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("account_id")
w9 = Window.partitionBy("merchant").orderBy("is_new_session", "id").rowsBetween(-5, Window.currentRow)
w10 = Window.partitionBy("account_id", "merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)

filtered = (
    df_transactions
    .filter(F.col("is_flagged"))
    .withColumn("txn_time_dayofweek", F.dayofweek("txn_time"))
    .withColumn("resolved_merchant", F.coalesce("merchant", F.lit("OTHER")))
)

flagged = (
    filtered
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("is_new_session").over(w2))

prev_events = islands.withColumn("prev_txn_time", F.lag("txn_time").over(w3))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands_transactions = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w4))

deduped = (
    islands_transactions
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

latest = (
    deduped
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
)

enriched = (
    latest
    .withColumn("last_known_merchant", F.last("merchant", ignorenulls=True).over(w7))
    .withColumn("account_id_sum_new_group_flag", F.sum("new_group_flag").over(w8))
    .withColumn("amount_avg_5", F.avg("amount").over(w9))
    .withColumn("session_no_avg_7d", F.avg("session_no").over(w10))
)

result = (
    enriched
    .groupBy("island_id")
    .agg(
        F.round(F.sum("txn_time_dayofweek") / F.nullif(F.count(F.when(F.col("merchant") == "7-Eleven", 1)), F.lit(0)), 2).alias("txn_time_dayofweek_per_7_eleven"),
        F.max("txn_time").alias("last_txn_time"),
        F.count(F.when(F.col("merchant") == "Amazon", 1)).alias("amazon_count"),
        F.min("txn_time").alias("first_txn_time"),
        F.sum(F.when(F.col("merchant") == "Amazon", F.col("session_no"))).alias("amazon_total_session_no"),
    )
    .orderBy(F.col("island_id").desc_nulls_first())
)
