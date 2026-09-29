from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    islands
    .withColumn("session_no_tier", F.when(F.col("session_no") >= 8, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["account_id", "session_no_tier", "txn_type"], ["account_id", "session_no_tier"], ["account_id"]],
        "account_id",
        "session_no_tier",
        "txn_type",
    )
    .agg(
        F.round(F.sum("session_start_flag") / F.nullif(F.count(F.when(F.col("merchant") == "Lazada", 1)), F.lit(0)), 2).alias("session_start_flag_per_lazada"),
        F.max("txn_time").alias("ended_at"),
        F.min("txn_time").alias("first_txn_time"),
    )
    .orderBy(F.col("session_start_flag_per_lazada").asc_nulls_first())
)
