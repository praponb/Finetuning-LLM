from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter(~F.col("merchant").eqNullSafe(F.lit("Grab")))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.lower("merchant").alias("merchant_lower"),
        F.when(F.col("txn_type") == "DEBIT", 1).otherwise(0).alias("is_debit"),
    )
)

with_gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_transactions
    .groupBy(
        F.when(F.col("is_new_burst") >= 1, "PREMIUM").otherwise("STANDARD").alias("is_new_burst_tier"),
    )
    .agg(
        F.percentile_approx("island_id", [0.25, 0.5, 0.75]).alias("island_id_quartiles"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
    )
    .orderBy(F.col("is_new_burst_tier").asc_nulls_first())
)
