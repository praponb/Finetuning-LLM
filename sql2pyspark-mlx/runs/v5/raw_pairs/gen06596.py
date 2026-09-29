from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-09-01").cast("date")) & F.col("country").isNotNull())
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        F.dayofmonth("started_at").alias("started_at_day"),
        F.coalesce("browser", F.lit("N/A")).alias("browser_filled"),
    )
)

with_gaps = (
    cleaned
    .withColumn("new_group_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("started_at_day") >= 67, "LARGE").otherwise("SMALL").alias("started_at_day_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
    )
    .orderBy(F.col("span_minutes").desc())
)
