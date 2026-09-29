from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .groupBy("burst_id")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("min_amount") >= 0)
    .orderBy("avg_amount")
)
