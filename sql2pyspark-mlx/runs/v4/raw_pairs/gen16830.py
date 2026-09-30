from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-11-18 14:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .select("*", F.col("duration_sec").cast("string").alias("duration_sec_num"))
)

flagged = (
    base
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessions_web_events
    .groupBy(
        F.when(F.col("session_start_flag") >= 1, "PREMIUM").otherwise("STANDARD").alias("session_start_flag_tier"),
        F.date_trunc("quarter", "event_time").alias("quarter_start"),
    )
    .agg(
        F.percentile_approx("session_no", [0.25, 0.5, 0.75]).alias("session_no_quartiles"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("event_type") == "add_to_cart", 1)), F.lit(0)), 2).alias("duration_sec_per_add_to_cart"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("page") == "/cart", 1)).alias("cart_rows"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("duration_sec_per_add_to_cart").desc())
)
