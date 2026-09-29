from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("txn_time", "id")
w3 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy("account_id").orderBy(F.col("amount").asc_nulls_last())
w5 = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w7 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w9 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("week_start").orderBy(F.col("avg_next_amount").desc())

enriched = (
    df_transactions
    .withColumn("first_known_merchant", F.first("merchant", ignorenulls=True).over(w))
    .withColumn("next_amount", F.lead("amount", 1).over(w2))
    .withColumn("amount_count_7d", F.count("amount").over(w3))
    .withColumn("amount_rank", F.rank().over(w4))
    .withColumn("running_amount", F.sum("amount").over(w5))
)

with_gaps = (
    enriched
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w6).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w6) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w7))

flagged = (
    sessions_transactions
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w8).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w8) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id_2", F.sum("is_new_burst").over(w9))

result = (
    islands
    .groupBy(
        F.date_trunc("WEEK", "txn_time").alias("week_start"),
        F.when(F.col("burst_id") >= 7, "PREMIUM").otherwise("STANDARD").alias("burst_id_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        F.round(F.avg("next_amount"), 2).alias("avg_next_amount"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
    )
    .withColumn("avg_next_amount_percent_rank", F.percent_rank().over(w10))
)
