from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter((F.col("merchant").eqNullSafe(F.lit("Lazada"))) & (F.col("amount") > 2700))
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.upper("merchant").alias("merchant_upper"),
        F.coalesce("merchant", F.lit("OTHER")).alias("resolved_merchant"),
    )
)

gaps = (
    valid
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_transactions = (
    gaps
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "merchant_upper",
        "resolved_merchant",
        F.sum("new_group_flag").over(w2).alias("island_id"),
    )
)

result = (
    sessions_transactions
    .groupBy(
        F.when(F.col("island_id") >= 7, "LARGE").otherwise("SMALL").alias("island_id_tier"),
    )
    .agg(
        F.max("txn_time").alias("ended_at"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("merchant") == "Amazon", 1)), F.lit(0)), 2).alias("amount_per_amazon"),
        F.count_if(F.col("merchant") == "Shell").alias("shell_events"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("merchant") == "Lazada", 1)), F.lit(0)), 2).alias("amount_per_lazada"),
    )
)
