from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (~F.col("merchant").eqNullSafe(F.lit("Starbucks"))))
    .select(
        "*",
        F.when(F.col("amount") >= 4300, "High").when(F.col("amount") >= 1900, "Medium").otherwise("Low").alias("amount_band"),
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("merchant_clean"),
    )
)

prev_events = cleaned.withColumn("prev_txn_time", F.lag("txn_time").over(w))

flagged = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .cube(
        F.date_trunc("MONTH", "txn_time").alias("activity_month"),
        F.date_trunc("WEEK", "prev_txn_time").alias("week_start"),
    )
    .agg(
        F.round(F.avg("is_new_burst"), 2).alias("avg_is_new_burst"),
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
    )
)
