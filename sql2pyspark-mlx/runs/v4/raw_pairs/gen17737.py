from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w))

flagged = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .select(
        "id",
        "account_id",
        "txn_type",
        "merchant",
        "is_flagged",
        "is_new_burst",
        F.when(F.col("merchant").rlike("^(Lazada|Shell)$"), "CORE").when(F.col("merchant").rlike("^(Grab|7-Eleven)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("merchant_zone"),
    )
    .orderBy("account_id")
)
