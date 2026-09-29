from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .withColumn("burst_id_tier", F.when(F.col("burst_id") >= 7, "HIGH").otherwise("LOW"))
    .withColumn("quarter_start", F.date_trunc("quarter", "txn_time"))
    .groupingSets(
        [["burst_id_tier", "quarter_start"], ["burst_id_tier"], []],
        "burst_id_tier",
        "quarter_start",
    )
    .agg(
        F.count(F.when(F.col("merchant") == "Starbucks", 1)).alias("starbucks_rows"),
        F.count(F.when(F.col("merchant") == "Starbucks", 1)).alias("starbucks_rows_2"),
        F.round(F.sum("session_start_flag") / F.nullif(F.count(F.when(F.col("txn_type") == "FEE", 1)), F.lit(0)), 2).alias("session_start_flag_per_fee"),
        F.count("*").alias("num_rows"),
        F.min("txn_time").alias("first_txn_time"),
    )
)
