from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter((F.col("started_at") >= F.to_date(F.lit("2023-06-01"))) & (F.col("country").eqNullSafe(F.lit("UK"))))
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        F.trim("browser").alias("clean_browser"),
        F.coalesce("country", F.lit("DIRECT")).alias("resolved_country"),
    )
)

flagged = (
    cleaned
    .withColumn("is_new_session", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        "clean_browser",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

result = (
    islands
    .groupBy(F.date_trunc("WEEK", "started_at").alias("week_start"), "browser")
    .agg(
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.count_if(F.col("country") == "DE").alias("de_events"),
        F.percentile_approx("island_id", [0.25, 0.5, 0.75]).alias("island_id_quartiles"),
    )
    .orderBy(F.col("week_start").desc())
)
