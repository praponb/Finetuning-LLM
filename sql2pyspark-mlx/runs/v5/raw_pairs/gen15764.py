from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_transactions
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "is_new_burst",
        "burst_id",
        F.when(F.col("merchant").rlike("^(Lazada|Shell)$"), "OTHER_ZONE").when(F.col("merchant").rlike("^(Grab|7-Eleven)$"), "GROUP_A").otherwise("GROUP_B").alias("merchant_zone"),
    )
    .orderBy("txn_time")
)
