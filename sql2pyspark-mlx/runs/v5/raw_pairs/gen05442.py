from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter(F.col("started_at") >= F.to_timestamp(F.lit("2023-08-07 08:00:00")))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.date_format("started_at", "yyyy-MM-dd").alias("started_at_label"),
    )
)

flagged = (
    filtered
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_sessions = flagged.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_sessions
    .rollup(F.date_trunc("QUARTER", "started_at").alias("quarter_start"), "customer_id")
    .agg(
        F.avg(F.when(F.col("device") == "mobile", F.col("is_new_burst"))).alias("mobile_avg_is_new_burst"),
        F.round(F.sum("island_id") / F.nullif(F.count(F.when(F.col("browser") == "Chrome", 1)), F.lit(0)), 2).alias("island_id_per_chrome"),
        F.avg(F.when(F.col("country") == "JP", F.col("island_id"))).alias("jp_avg_island_id"),
    )
    .orderBy("mobile_avg_is_new_burst")
)
