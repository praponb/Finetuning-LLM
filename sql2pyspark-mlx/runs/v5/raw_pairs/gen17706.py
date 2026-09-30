from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("prev_paid_at", "id")
w_running = Window.partitionBy("status", "order_id").orderBy(F.col("prev_paid_at").desc(), "id")
w5 = Window.partitionBy("status", "order_id").orderBy("amount", "id")

filtered = (
    df_payments
    .filter(F.col("order_id").isin([13, 25, 40]))
    .select("*", F.hour("paid_at").alias("paid_at_hour"))
)

with_prev = filtered.withColumn("prev_paid_at", F.lag("paid_at").over(w))

flagged = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("new_group_flag").over(w2))

scored = (
    islands
    .select(
        "*",
        F.lead("island_id", 1).over(w3).alias("next_island_id"),
        F.row_number().over(w_running).alias("row_seq"),
    )
)

valid = (
    scored
    .filter((F.col("paid_at") > F.lit("2023-08-30").cast("date")) & (F.col("method") == "promptpay"))
    .select(
        "*",
        F.round("row_seq", 0).alias("row_seq_rounded"),
        (F.col("next_island_id") * F.col("paid_at_hour")).alias("next_island_id_times_paid_at_hour"),
        F.coalesce("method", F.lit("N/A")).alias("method_filled"),
    )
)

ranked = (
    valid
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("activity_month"))
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("island_id", [0.25, 0.5, 0.75]).alias("island_id_quartiles"),
        F.sum(F.when(F.col("status") == "REFUNDED", F.col("paid_at_hour")).otherwise(0)).alias("refunded_paid_at_hour"),
    )
    .filter(F.col("row_count") < 1000)
)
