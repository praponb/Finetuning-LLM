from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("browser").orderBy("started_at", "id")

valid = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-09-17").cast("date")) & F.col("customer_id").isNotNull() & ~F.col("device").isin(["tablet", "desktop", "mobile"]))
    .withColumn("device_lower", F.lower("device"))
)

metrics = (
    valid
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w3))
)

with_gaps = (
    metrics
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w4).isNull() | (F.col("started_at") > F.lag("started_at").over(w4) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("session_start_flag").over(w3))

grouped = (
    islands
    .groupBy(F.date_trunc("MONTH", "started_at").alias("activity_month"))
    .agg(
        F.sum(F.when(F.col("device") == "tablet", F.col("event_seq")).otherwise(0)).alias("tablet_event_seq"),
        F.count("*").alias("row_count"),
        F.size(F.collect_set("country")).alias("num_countries"),
    )
)

result = (
    grouped
    .select("activity_month", "tablet_event_seq", "num_countries")
    .orderBy(F.col("activity_month").desc_nulls_first())
)
