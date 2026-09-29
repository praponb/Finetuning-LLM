from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter((F.col("started_at") >= "2023-04-01") & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.concat_ws("-", "device", "country").alias("device_country_key"),
        F.coalesce("browser", F.lit("UNKNOWN")).alias("resolved_browser"),
    )
)

flagged = (
    filtered
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "started_at",
        "country",
        "device_country_key",
        F.sum("is_new_burst").over(w2).alias("island_id"),
    )
)

result = (
    sessionized
    .groupBy(F.date_trunc("MONTH", "started_at").alias("activity_month"), "island_id")
    .agg(
        F.min("started_at").alias("started_at"),
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
    )
)
