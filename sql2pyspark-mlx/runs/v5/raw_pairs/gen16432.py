from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("burst_id_tier").orderBy(F.col("row_count").desc())

filtered = (
    df_sessions
    .filter(F.col("country").isin(["UK", "DE", "SG"]))
    .withColumn("started_at_date", F.to_date("started_at"))
    .withColumn("browser_title", F.initcap(F.trim("browser")))
    .withColumn("browser_clean", F.coalesce("browser", F.lit("OTHER")))
)

with_prev = filtered.withColumn("prev_started_at", F.lag("started_at").over(w))

with_gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_sessions
    .groupBy(
        F.when(F.col("burst_id") >= 9, "HIGH").otherwise("LOW").alias("burst_id_tier"),
        F.date_trunc("YEAR", "prev_started_at").alias("year_start"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("burst_id", [0.25, 0.5, 0.75]).alias("burst_id_quartiles"),
        ((F.unix_timestamp(F.max("prev_started_at")) - F.unix_timestamp(F.min("prev_started_at"))) / 60).alias("duration_min"),
        F.percentile_approx("burst_id", [0.25, 0.5, 0.75]).alias("burst_id_quartiles_2"),
        F.round(F.avg("burst_id"), 2).alias("avg_burst_id"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("duration_min") < 1000000)
    .withColumn("row_count_percent_rank", F.percent_rank().over(w3))
    .orderBy(F.col("cnt").desc_nulls_first())
)
