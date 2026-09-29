from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(-2, -1)
w_running = Window.partitionBy("country").orderBy("is_chrome", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy("country").orderBy(F.col("is_chrome_count_7d").desc_nulls_first(), "id")

cleaned = (
    df_sessions
    .filter(F.col("started_at") >= F.to_timestamp(F.lit("2023-06-29 03:00:00")))
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.when(F.col("browser") == "Chrome", 1).otherwise(0).alias("is_chrome"),
        F.coalesce("country", F.lit("NONE")).alias("resolved_country"),
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "customer_id",
        "device",
        "country",
        "resolved_country",
        F.avg("is_chrome").over(w).alias("is_chrome_avg_2_prior"),
        F.last("is_chrome", ignorenulls=True).over(w_running).alias("last_known_is_chrome"),
        F.count("is_chrome").over(w3).alias("is_chrome_count_7d"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.page").isin(["/product", "/search"])), "left_anti")
)

deduped = (
    kept
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

grouped = (
    deduped
    .groupBy("country")
    .agg(
        F.max(F.when(F.col("device") == "desktop", F.col("last_known_is_chrome"))).alias("desktop_max_last_known_is_chrome"),
        F.round(F.sum("is_chrome_count_7d") / F.nullif(F.count(F.when(F.col("country") == "SG", 1)), F.lit(0)), 2).alias("is_chrome_count_7d_per_sg"),
    )
)

result = grouped.select("country")
