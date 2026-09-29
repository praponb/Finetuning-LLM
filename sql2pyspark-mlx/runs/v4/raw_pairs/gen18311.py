from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("is_sg_tier").orderBy(F.col("is_sg_p50").desc())

base = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-08-30 09:00:00").cast("timestamp")) & F.col("browser").isNotNull() & F.col("country").isin(["US", "UK"]))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.date_format("started_at", "yyyy-MM").alias("started_at_label"),
        F.col("started_at").cast("date").alias("started_at_day"),
    )
)

base_sessions = (
    base
    .filter(F.col("country") == "TH")
    .select(
        "*",
        F.last_day("started_at_day").alias("started_at_day_month_end"),
        F.when(F.col("country") == "SG", 1).otherwise(0).alias("is_sg"),
        F.coalesce("browser", F.lit("UNKNOWN")).alias("browser_filled"),
    )
)

base_sessions_2 = (
    base_sessions
    .filter((F.col("started_at") > F.lit("2023-12-14").cast("date")) & (F.year("started_at_day_month_end") == 2025))
    .select(
        "id",
        "device",
        "started_at",
        "started_at_label",
        "is_sg",
        "browser_filled",
        F.trim("device").alias("clean_device"),
        F.initcap(F.trim("device")).alias("device_title"),
        F.coalesce("browser", F.lit("DIRECT")).alias("resolved_browser"),
    )
)

result = (
    base_sessions_2
    .groupBy(
        F.when(F.col("is_sg") >= 65, "LARGE").otherwise("SMALL").alias("is_sg_tier"),
        "device",
    )
    .agg(
        F.max("is_sg").alias("max_is_sg"),
        F.percentile_approx("is_sg", [0.25, 0.5, 0.75]).alias("is_sg_quartiles"),
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_count"),
        F.min("is_sg").alias("min_is_sg"),
        F.percentile_approx("is_sg", 0.5).alias("is_sg_p50"),
        F.round(F.sum("is_sg") / F.nullif(F.count(F.when(F.col("device") == "mobile", 1)), F.lit(0)), 2).alias("is_sg_per_mobile"),
    )
    .filter(F.col("is_sg_per_mobile") < 0)
    .withColumn("is_sg_p50_rank", F.rank().over(w))
    .orderBy(F.col("min_is_sg").desc_nulls_last())
)
