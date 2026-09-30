from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at_hour").desc_nulls_first())
w2 = Window.orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("browser").orderBy("started_at_hour_sum_7d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("browser").orderBy("started_at_hour", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("browser")

base = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-07-15 11:00:00").cast("timestamp")) & F.col("country").isNotNull() & ~F.col("browser").isin(["Safari", "Firefox"]))
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        F.hour("started_at").alias("started_at_hour"),
        F.coalesce("country", F.lit("OTHER")).alias("country_filled"),
    )
)

scored = (
    base
    .select(
        "*",
        F.percent_rank().over(w).alias("started_at_hour_percent_rank"),
        F.sum("started_at_hour").over(w2).alias("started_at_hour_sum_7d"),
    )
)

scored_sessions = (
    scored
    .select(
        "*",
        F.sum("started_at_hour").over(w3).alias("cumulative_started_at_hour"),
        F.last("started_at_hour_sum_7d", ignorenulls=True).over(w4).alias("last_known_started_at_hour_sum_7d"),
        F.count("started_at_hour_sum_7d").over(w5).alias("browser_count_started_at_hour_sum_7d"),
    )
)

daily = (
    scored_sessions
    .groupBy(
        F.date_trunc("year", "started_at").alias("year_start"),
        "browser",
        "country",
    )
    .agg(
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.percentile_approx("started_at_hour", [0.25, 0.5, 0.75]).alias("started_at_hour_quartiles"),
        F.count("*").alias("row_count"),
    )
)

result = (
    daily
    .select(
        "year_start",
        "browser",
        "country",
        "browser_list",
        "country_list",
        "started_at_hour_quartiles",
        "row_count",
    )
    .orderBy("row_count")
)
