from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-06-16").cast("date")) & F.col("customer_id").isNotNull() & (F.col("device") != "desktop"))
    .withColumn("device_title", F.initcap(F.trim("device")))
    .withColumn("resolved_browser", F.coalesce("browser", F.lit("UNKNOWN")))
)

with_gaps = (
    cleaned
    .withColumn("new_group_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    sessions_sessions
    .groupBy(
        F.when(F.col("island_id") >= 6, "LARGE").otherwise("SMALL").alias("island_id_tier"),
        "browser",
    )
    .agg(
        F.size(F.collect_set("device")).alias("num_devices"),
        F.avg("island_id").alias("avg_island_id"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.min("island_id").alias("min_island_id"),
    )
)
