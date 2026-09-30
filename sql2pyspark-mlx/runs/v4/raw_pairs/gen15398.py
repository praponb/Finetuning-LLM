from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .groupBy(
        "txn_type",
        F.date_trunc("WEEK", "txn_time").alias("week_start"),
        "account_id",
    )
    .agg(
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.min("txn_time").alias("first_seen"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list_2"),
        F.min("txn_time").alias("started_at"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .filter(F.col("avg_amount") > 0)
    .orderBy(F.col("week_start").desc())
)
