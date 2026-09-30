from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .withColumn("island_id_tier", F.when(F.col("island_id") >= 9, "PREMIUM").otherwise("STANDARD"))
    .rollup("island_id_tier")
    .agg(
        F.round(F.avg("is_new_burst"), 2).alias("avg_is_new_burst"),
        F.min("prev_txn_time").alias("first_seen"),
        F.sum("is_new_burst").alias("total_is_new_burst"),
        F.grouping("island_id_tier").alias("grouping_id"),
    )
    .orderBy(F.col("total_is_new_burst").asc_nulls_last())
)
