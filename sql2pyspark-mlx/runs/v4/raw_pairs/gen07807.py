from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        "burst_id",
        F.when(F.col("txn_type").rlike("^(CREDIT|DEBIT)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("txn_type_zone"),
    )
)
