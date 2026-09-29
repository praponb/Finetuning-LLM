from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.sum("new_group_flag").over(w2).alias("island_id"),
    )
)

result = (
    islands
    .select(
        "id",
        "txn_type",
        "amount",
        "is_flagged",
        F.when(F.col("txn_type").rlike("^(CREDIT|DEBIT)$"), "GROUP_B").when(F.col("txn_type").rlike("^(REFUND|FEE)$"), "OTHER_ZONE").otherwise("CORE").alias("txn_type_zone"),
    )
)
