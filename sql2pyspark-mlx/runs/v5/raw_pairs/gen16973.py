from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter(F.col("txn_time") >= F.lit("2023-08-13").cast("date"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.substring("merchant", 1, 2).alias("merchant_prefix"),
        F.length("txn_type").alias("txn_type_length"),
    )
)

flagged = (
    filtered
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_transactions
    .groupBy(F.date_trunc("YEAR", "txn_time").alias("year_start"))
    .agg(
        F.sum(F.when(F.col("merchant") == "Amazon", F.col("txn_type_length"))).alias("amazon_total_txn_type_length"),
        F.min("amount").alias("min_amount"),
        F.max("txn_time").alias("last_txn_time"),
    )
    .orderBy(F.col("min_amount").desc_nulls_last())
)
