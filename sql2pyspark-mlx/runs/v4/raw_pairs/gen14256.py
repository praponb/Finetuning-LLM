from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w))

flagged = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    sessions_transactions
    .groupBy(
        F.date_trunc("QUARTER", "txn_time").alias("quarter_start"),
        F.date_trunc("YEAR", "prev_txn_time").alias("year_start"),
    )
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.avg("is_new_session").alias("avg_is_new_session"),
        F.avg("amount").alias("avg_amount_2"),
    )
)
