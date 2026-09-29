from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2024-01-28").cast("date"))
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        F.when(F.col("device").isin(["desktop", "tablet"]), "Tier 1").otherwise("Other").alias("device_group"),
        F.coalesce("country", F.lit("OTHER")).alias("resolved_country"),
    )
)

gaps = (
    cleaned
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessions_sessions
    .groupBy(
        F.when(F.col("session_no") >= 7, "HIGH").otherwise("LOW").alias("session_no_tier"),
        F.when(F.col("session_start_flag") >= 1, "HIGH").otherwise("LOW").alias("session_start_flag_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.percentile_approx("session_start_flag", 0.9).alias("session_start_flag_p90"),
    )
)
