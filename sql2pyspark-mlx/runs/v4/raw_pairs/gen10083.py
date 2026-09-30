from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-07-13").cast("date")) & F.col("browser").isNotNull())
    .select(
        "*",
        F.length("browser").alias("browser_length"),
        F.coalesce("country", F.lit("N/A")).alias("country_clean"),
    )
)

prev_events = cleaned.withColumn("prev_started_at", F.lag("started_at").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_sessions
    .groupBy("browser")
    .agg(
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.max("is_new_burst").alias("max_is_new_burst"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.percentile_approx("browser_length", [0.25, 0.5, 0.75]).alias("browser_length_quartiles"),
    )
)
