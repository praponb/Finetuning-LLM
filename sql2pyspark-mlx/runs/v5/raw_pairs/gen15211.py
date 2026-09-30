from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy(F.col("started_at").desc_nulls_first(), "id")

cleaned = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2023-08-14 07:00:00").cast("timestamp"))
    .withColumn("country_title", F.initcap(F.trim("country")))
)

filtered = (
    cleaned
    .filter(~F.col("country").eqNullSafe(F.lit("TH")))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "country_title",
        F.when(F.col("browser").isin(["Chrome", "Edge"]), "Group A").otherwise("Other").alias("browser_group"),
        F.when(F.col("browser").isin(["Edge", "Firefox"]), "Core").otherwise("Other").alias("browser_group_2"),
        F.coalesce("browser", F.lit("OTHER")).alias("browser_filled"),
    )
)

with_prev = filtered.withColumn("prev_started_at", F.lag("started_at").over(w))

with_gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

ranked = (
    islands
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .groupBy("browser", "device", "country")
    .agg(
        F.round(F.sum("session_no") / F.nullif(F.count(F.when(F.col("browser") == "Chrome", 1)), F.lit(0)), 2).alias("session_no_per_chrome"),
        F.min("started_at").alias("first_started_at"),
        F.percentile_approx("session_start_flag", 0.5).alias("session_start_flag_p50"),
    )
    .orderBy(F.col("country").asc_nulls_last())
)
