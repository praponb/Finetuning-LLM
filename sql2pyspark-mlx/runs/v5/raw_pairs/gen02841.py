from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter((F.col("merchant").eqNullSafe(F.lit("Lazada"))) & F.col("is_flagged"))
    .select(
        "*",
        F.lower("merchant").alias("merchant_lower"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("resolved_merchant"),
    )
)

gaps = (
    valid
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        "resolved_merchant",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

result = (
    islands
    .withColumn("week_start", F.date_trunc("week", "txn_time"))
    .groupingSets(
        [["week_start", "merchant", "account_id"], ["week_start", "merchant"], ["week_start"]],
        "week_start",
        "merchant",
        "account_id",
    )
    .agg(
        F.max("txn_time").alias("last_seen"),
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_rows"),
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("island_id")).otherwise(0)).alias("fee_island_id"),
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
        F.grouping_id().alias("gid"),
    )
)
