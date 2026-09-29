from pyspark.sql import functions as F

base = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-10-04 15:00:00").cast("timestamp")) & F.col("customer_id").isNotNull() & (F.col("customer_id") == 12))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.when(F.col("browser") == "Safari", 1).otherwise(0).alias("is_safari"),
    )
)

result = (
    base
    .groupBy(F.date_trunc("MONTH", "started_at").alias("activity_month"))
    .agg(
        F.count_if(F.col("device") == "mobile").alias("mobile_events"),
        F.round(F.sum("is_safari") / F.nullif(F.count(F.when(F.col("browser") == "Chrome", 1)), F.lit(0)), 2).alias("is_safari_per_chrome"),
        F.percentile_approx("is_safari", 0.5).alias("is_safari_p50"),
        F.round(F.avg("is_safari"), 2).alias("avg_is_safari"),
    )
    .orderBy("is_safari_p50")
)
