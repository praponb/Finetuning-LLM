from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("is_new_burst", "id")
w4 = Window.partitionBy("device").orderBy("is_new_burst", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("device").orderBy("prev_started_at", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")
w7 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter(F.col("browser").isNotNull())
    .select(
        "*",
        F.concat_ws("-", "browser", "device").alias("browser_device_key"),
        F.date_format("started_at", "yyyy-MM").alias("started_at_label"),
        F.coalesce("country", F.lit("NONE")).alias("country_clean"),
    )
)

prev_events = base.withColumn("prev_started_at", F.lag("started_at").over(w))

gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

scored = (
    sessionized
    .select(
        "*",
        F.row_number().over(w3).alias("event_seq"),
        F.first("country", ignorenulls=True).over(w4).alias("first_known_country"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at_label",
        "country_clean",
        "prev_started_at",
        "island_id",
        "event_seq",
        F.avg("island_id").over(w_running).alias("island_id_avg_5"),
        F.row_number().over(w6).alias("event_seq_2"),
        F.sum("island_id").over(w7).alias("cumulative_island_id"),
    )
)

result = (
    enriched
    .groupBy("browser", "customer_id")
    .pivot("device", ["desktop", "tablet"])
    .agg(F.avg("island_id_avg_5").alias("amount"))
)
