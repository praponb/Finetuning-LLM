from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-03-19").cast("date")) & (~F.col("country").eqNullSafe(F.lit("DE"))))
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        F.lower("browser").alias("browser_lower"),
        F.when(F.col("browser") == "Safari", 1).otherwise(0).alias("is_safari"),
        F.coalesce("country", F.lit("OTHER")).alias("resolved_country"),
    )
)

flagged = (
    filtered
    .withColumn("new_group_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .groupBy("country")
    .agg(
        F.percentile_approx("is_safari", 0.95).alias("is_safari_p95"),
        F.sum(F.when(F.col("country") == "JP", F.col("burst_id")).otherwise(0)).alias("jp_burst_id"),
        F.count_if(F.col("country") == "DE").alias("de_events"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("jp_burst_id") > 0)
    .orderBy(F.col("row_count").desc_nulls_first())
)
