from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("activity_month").orderBy(F.col("amount_stddev").desc())

with_prev = df_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w))

with_gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .groupBy(
        F.date_trunc("MONTH", "prev_txn_time").alias("activity_month"),
        F.when(F.col("island_id") >= 8, "HIGH").otherwise("LOW").alias("island_id_tier"),
    )
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.min("prev_txn_time").alias("first_seen"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list_2"),
    )
    .filter(F.col("amount_stddev") >= 0)
    .withColumn("amount_stddev_rank", F.rank().over(w3))
)
