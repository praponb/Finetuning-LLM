from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-06-05 12:00:00").cast("timestamp")) & F.col("device").isin(["desktop", "mobile", "tablet"]))
    .withColumn("country_upper", F.upper("country"))
    .withColumn("browser_clean", F.coalesce("browser", F.lit("UNKNOWN")))
)

gaps = (
    valid
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        "country_upper",
        "browser_clean",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

result = (
    islands
    .rollup(F.date_trunc("WEEK", "started_at").alias("week_start"), "device")
    .agg(
        F.percentile_approx("island_id", 0.5).alias("island_id_p50"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.avg(F.when(F.col("country") == "SG", F.col("island_id"))).alias("sg_avg_island_id"),
    )
    .filter(F.col("island_id_p50") > 0)
    .orderBy(F.col("device").desc())
)
