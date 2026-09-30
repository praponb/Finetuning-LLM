from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2023-08-11 11:00:00").cast("timestamp"))
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.when(F.col("browser") == "Chrome", 1).otherwise(0).alias("is_chrome"),
    )
)

with_prev = valid.withColumn("prev_started_at", F.lag("started_at").over(w))

flagged = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "device",
        "started_at",
        "country",
        "is_chrome",
        F.sum("new_group_flag").over(w2).alias("island_id"),
    )
)

result = (
    islands
    .groupBy(F.date_trunc("quarter", "started_at").alias("quarter_start"))
    .agg(
        F.sum(F.when(F.col("device") == "tablet", F.col("is_chrome")).otherwise(0)).alias("tablet_is_chrome"),
        F.min("is_chrome").alias("min_is_chrome"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.count_if(F.col("device") == "desktop").alias("desktop_events"),
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
        F.max(F.when(F.col("device") == "tablet", F.col("island_id"))).alias("tablet_max_island_id"),
    )
    .orderBy(F.col("desktop_events").asc_nulls_last())
)
