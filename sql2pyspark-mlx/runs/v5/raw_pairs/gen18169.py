from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .groupBy(F.date_trunc("MONTH", "prev_txn_time").alias("activity_month"))
    .agg(
        F.sum(F.when(F.col("merchant") == "Grab", F.col("session_no")).otherwise(0)).alias("grab_session_no"),
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("session_no")).otherwise(0)).alias("debit_session_no"),
        F.round(F.avg("is_new_burst"), 2).alias("avg_is_new_burst"),
    )
    .filter(F.col("avg_is_new_burst") > 0)
)
