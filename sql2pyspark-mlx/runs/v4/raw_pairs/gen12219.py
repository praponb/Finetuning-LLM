from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-06-29 22:00:00").cast("timestamp")) & F.col("is_flagged"))
    .select(
        "*",
        F.when(F.col("txn_type") == "DEBIT", 1).otherwise(0).alias("is_debit"),
        F.coalesce("merchant", F.lit("N/A")).alias("resolved_merchant"),
    )
)

gaps = (
    valid
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_transactions
    .groupBy("island_id", F.date_trunc("year", "txn_time").alias("year_start"))
    .agg(
        F.max("txn_time").alias("last_seen"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.min("txn_time").alias("first_seen"),
    )
    .orderBy(F.col("year_start").desc_nulls_first())
)
