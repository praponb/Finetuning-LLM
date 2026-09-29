from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("days_since_started_at").desc())
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id")
w3 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("browser").orderBy("started_at", "id")
w5 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter(F.col("started_at") >= F.to_date(F.lit("2023-04-21")))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.datediff(F.current_date(), "started_at").alias("days_since_started_at"),
        F.coalesce("browser", F.lit("OTHER")).alias("browser_filled"),
    )
)

ranked = (
    cleaned
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_gaps = (
    ranked
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w2).isNull() | (F.col("started_at") > F.lag("started_at").over(w2) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("is_new_burst").over(w3))

gaps = (
    sessionized
    .withColumn("is_new_burst_2", F.when(F.col("started_at") - F.lag("started_at").over(w4) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("session_id", F.sum("is_new_burst_2").over(w5))

result = (
    sessions_sessions
    .groupBy(F.date_trunc("YEAR", "started_at").alias("year_start"))
    .agg(
        F.percentile_approx("is_new_burst", 0.5).alias("is_new_burst_p50"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.max(F.when(F.col("browser") == "Chrome", F.col("is_new_burst_2"))).alias("chrome_max_is_new_burst_2"),
        F.round(F.sum("session_id") / F.nullif(F.count(F.when(F.col("browser") == "Chrome", 1)), F.lit(0)), 2).alias("session_id_per_chrome"),
    )
    .filter(F.col("num_countries") < 1)
)
