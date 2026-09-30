from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "*",
        F.date_format("txn_time", "MMM yyyy").alias("txn_time_label"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("merchant_clean"),
    )
)

gaps = (
    base
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("amount") >= 4200, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
        F.when(F.col("island_id") >= 6, "HIGH").otherwise("LOW").alias("island_id_tier"),
    )
    .agg(
        F.percentile_approx("island_id", 0.5).alias("island_id_p50"),
        F.percentile_approx("island_id", [0.25, 0.5, 0.75]).alias("island_id_quartiles"),
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_count"),
        F.sum(F.when(F.col("merchant") == "Shell", F.col("island_id")).otherwise(0)).alias("shell_island_id"),
    )
    .orderBy(F.col("amount_tier").asc_nulls_last())
)
