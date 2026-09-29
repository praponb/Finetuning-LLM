from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter((F.col("started_at") >= F.to_date(F.lit("2023-03-26"))) & F.col("country").isNotNull())
    .withColumn("country_lower", F.lower("country"))
    .withColumn("resolved_country", F.coalesce("country", F.lit("UNKNOWN")))
)

with_gaps = (
    cleaned
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .groupBy("browser")
    .agg(
        F.min("started_at").alias("started_at"),
        F.avg(F.when(F.col("device") == "desktop", F.col("island_id"))).alias("desktop_avg_island_id"),
    )
    .filter(F.col("desktop_avg_island_id") > 0)
    .orderBy(F.col("browser").desc_nulls_last())
)
