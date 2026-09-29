from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        F.sum("session_start_flag").over(w2).alias("session_id"),
    )
)

result = (
    sessionized
    .select("id", "account_id", "merchant", "session_id")
    .orderBy(F.col("merchant").desc_nulls_first(), F.col("id").desc())
)
