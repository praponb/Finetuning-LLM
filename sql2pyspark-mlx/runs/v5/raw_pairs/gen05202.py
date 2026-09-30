from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("ends_on_period", "id")
w2 = Window.partitionBy("channel").orderBy("ends_on_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.col("starts_on") > F.lit("2023-04-15").cast("date")))
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        F.date_trunc("MONTH", "ends_on").alias("ends_on_period"),
    )
)

with_prev = filtered.withColumn("prev_ends_on_period", F.lag("ends_on_period").over(w))

flagged = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_ends_on_period").isNull() | (F.col("ends_on_period") > F.col("prev_ends_on_period") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "ends_on_period",
        F.sum("is_new_burst").over(w2).alias("burst_id"),
    )
)

flagged_2 = (
    islands
    .withColumn("is_new_session", F.when(F.col("ends_on_period") - F.lag("ends_on_period").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_promotions = flagged_2.withColumn("burst_id_2", F.sum("is_new_session").over(w2))

with_gaps = (
    sessions_promotions
    .withColumn("is_new_burst", F.when(F.lag("ends_on_period").over(w).isNull() | (F.col("ends_on_period") > F.lag("ends_on_period").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands_promotions = (
    with_gaps
    .select(
        "id",
        "channel",
        "ends_on_period",
        "burst_id_2",
        F.sum("is_new_burst").over(w2).alias("island_id"),
    )
)

result = (
    islands_promotions
    .select("id", "channel", "ends_on_period", "burst_id_2")
    .orderBy(F.col("channel").desc_nulls_last(), "id")
)
