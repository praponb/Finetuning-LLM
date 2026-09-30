from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter((F.col("txn_time") > F.to_timestamp(F.lit("2023-05-22 10:00:00"))) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        F.upper("merchant").alias("merchant_upper"),
        F.coalesce("merchant", F.lit("N/A")).alias("merchant_clean"),
    )
)

with_gaps = (
    base
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("session_start_flag") >= 1, "HIGH").otherwise("LOW").alias("session_start_flag_tier"),
    )
    .agg(
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("session_start_flag").alias("min_session_start_flag"),
    )
)
