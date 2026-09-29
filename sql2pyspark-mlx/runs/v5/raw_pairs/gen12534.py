from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w))

with_gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        "new_group_flag",
    )
    .orderBy("amount", "id")
)
