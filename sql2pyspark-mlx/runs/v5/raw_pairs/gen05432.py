from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w2 = Window.partitionBy("country", "customer_id").orderBy(F.col("started_at_hour").desc(), "id")
w3 = Window.partitionBy("device").orderBy("started_at_hour", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("browser").orderBy("started_at_hour", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("browser").orderBy("started_at_hour", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("country", "customer_id").orderBy(F.col("started_at").desc_nulls_last())
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w8 = Window.partitionBy("browser").orderBy(F.col("started_at_hour_count_7d").desc_nulls_first())
w9 = Window.partitionBy("country").orderBy("started_at_hour", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("page").orderBy("started_at_hour_count_7d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter(F.col("country").isin(["JP", "US", "UK"]))
    .select("*", F.hour("started_at").alias("started_at_hour"))
)

enriched = (
    cleaned
    .withColumn("started_at_hour_count_7d", F.count("started_at_hour").over(w))
    .withColumn("started_at_hour_bucket", F.ntile(4).over(w2))
    .withColumn("first_known_started_at_hour", F.first("started_at_hour", ignorenulls=True).over(w3))
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w4))
    .withColumn("started_at_hour_avg_6", F.avg("started_at_hour").over(w5))
)

ranked = (
    enriched
    .withColumn("rn", F.rank().over(w6))
    .filter(F.col("rn") == 1)
)

cleaned_sessions = (
    ranked
    .filter(F.col("browser").eqNullSafe(F.lit("Chrome")))
    .select(
        "*",
        (F.col("first_known_started_at_hour") - F.col("started_at_hour_avg_6")).alias("first_known_started_at_hour_minus_started_at_hour_avg_6"),
        F.coalesce("browser", F.lit("NONE")).alias("browser_filled"),
    )
)

latest_match = (
    cleaned_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left")
    .withColumn("rn_2", F.row_number().over(w7))
    .filter(F.col("rn_2") == 1)
    .select("s.*", F.col("c.event_time").alias("web_event_event_time"), F.col("c.page"))
)

metrics = (
    latest_match
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "started_at_hour",
        "started_at_hour_count_7d",
        "started_at_hour_bucket",
        "rn",
        "web_event_event_time",
        F.dense_rank().over(w8).alias("started_at_hour_count_7d_dense_rank"),
        F.first("country", ignorenulls=True).over(w9).alias("first_known_country"),
        F.sum("started_at_hour_bucket").over(w10).alias("cumulative_started_at_hour_bucket"),
    )
)

result = (
    metrics
    .withColumn("quarter_start", F.date_trunc("QUARTER", "started_at"))
    .withColumn("week_start", F.date_trunc("WEEK", "started_at"))
    .withColumn("started_at_hour_count_7d_tier", F.when(F.col("started_at_hour_count_7d") >= 12, "HIGH").otherwise("LOW"))
    .rollup("quarter_start", "week_start", "started_at_hour_count_7d_tier")
    .agg(
        F.min("web_event_event_time").alias("started_at"),
        F.round(F.sum("started_at_hour_count_7d") / F.nullif(F.count(F.when(F.col("device") == "desktop", 1)), F.lit(0)), 2).alias("started_at_hour_count_7d_per_desktop"),
        F.percentile_approx("cumulative_started_at_hour_bucket", 0.5).alias("cumulative_started_at_hour_bucket_p50"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.grouping("quarter_start").alias("gid"),
    )
    .orderBy(F.col("cumulative_started_at_hour_bucket_p50").desc_nulls_last())
)
