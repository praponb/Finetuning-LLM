from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .groupBy("account_id", F.date_trunc("WEEK", "txn_time").alias("week_start"))
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("merchant").alias("distinct_merchants"),
    )
    .orderBy(F.col("account_id").asc_nulls_last())
)
