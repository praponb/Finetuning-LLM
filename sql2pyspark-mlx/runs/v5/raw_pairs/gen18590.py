from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_transactions
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_transactions = (
    flagged
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

result = (
    sessions_transactions
    .cube("burst_id", "account_id")
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        F.max("txn_time").alias("last_seen"),
        F.max("txn_time").alias("last_seen_2"),
        F.grouping("burst_id").alias("grouping_level"),
    )
)
