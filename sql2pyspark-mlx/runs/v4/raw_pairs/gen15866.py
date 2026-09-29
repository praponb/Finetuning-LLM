from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("browser").orderBy("started_at", "id")

filtered = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-06-28 11:00:00").cast("timestamp")) & F.col("device").isin(["tablet", "desktop", "mobile"]))
    .select("*", F.concat("country", F.lit(", "), "device").alias("country_device"))
)

scored = (
    filtered
    .select(
        "*",
        F.last("browser", ignorenulls=True).over(w).alias("last_known_browser"),
        F.last("country", ignorenulls=True).over(w2).alias("last_known_country"),
        F.last("browser", ignorenulls=True).over(w3).alias("last_known_browser_2"),
    )
)

with_gaps = (
    scored
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w4) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("session_start_flag").over(w3))

result = (
    islands
    .groupBy(
        F.when(F.col("session_start_flag") >= 1, "PREMIUM").otherwise("STANDARD").alias("session_start_flag_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("burst_id", 0.95).alias("burst_id_p95"),
    )
    .filter(F.col("burst_id_p95") >= 0)
)
