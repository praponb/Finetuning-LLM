from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("is_flagged_flag").desc_nulls_last(), "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w3 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-07-05").cast("date")) & F.col("merchant").isNotNull())
    .select(
        "*",
        F.when(F.col("txn_type") == "FEE", 1).otherwise(0).alias("is_fee"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("resolved_merchant"),
    )
)

ranked = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_gaps = (
    ranked
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w2) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("is_new_session").over(w3))

result = (
    islands
    .groupBy(
        F.when(F.col("is_fee") >= 62, "HIGH").otherwise("LOW").alias("is_fee_tier"),
    )
    .agg(
        F.count(F.when(F.col("merchant") == "7-Eleven", 1)).alias("7_eleven_rows"),
        F.max("is_new_session").alias("max_is_new_session"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("txn_type") == "DEBIT", 1)), F.lit(0)), 2).alias("amount_per_debit"),
        F.round(F.sum("session_id") / F.nullif(F.count(F.when(F.col("merchant") == "Grab", 1)), F.lit(0)), 2).alias("session_id_per_grab"),
        F.percentile_approx("amount", 0.9).alias("amount_p90"),
        F.count(F.when(F.col("merchant") == "Starbucks", 1)).alias("starbucks_count"),
    )
    .orderBy(F.col("max_is_new_session").desc())
)
