from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter((F.col("txn_time") >= "2025-03-01") & (F.col("txn_time") >= F.lit("2023-08-03 02:00:00").cast("timestamp")))
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
)

flagged = (
    filtered
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessions_transactions
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("merchant") == "Starbucks", F.col("session_start_flag")).otherwise(0)).alias("starbucks_session_start_flag"),
        F.count(F.when(F.col("merchant") == "Lazada", 1)).alias("lazada_rows"),
        F.avg(F.when(F.col("txn_type") == "DEBIT", F.col("session_start_flag"))).alias("debit_avg_session_start_flag"),
        F.max(F.when(F.col("txn_type") == "CREDIT", F.col("amount"))).alias("credit_max_amount"),
        F.countDistinct("txn_type").alias("distinct_txn_types"),
    )
    .filter(F.col("distinct_txn_types") < 1000)
)
