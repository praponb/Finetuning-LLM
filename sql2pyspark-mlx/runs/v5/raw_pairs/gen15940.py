from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time_day").desc(), "id")
w2 = Window.partitionBy("browser").orderBy(F.col("event_time_day").desc(), "id")
w3 = Window.partitionBy("browser").orderBy(F.col("row_num").desc(), "id")
w4 = Window.partitionBy("session_id").orderBy("event_time_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("device").orderBy("event_time_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-10-14 13:00:00").cast("timestamp"))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.col("event_time").cast("date").alias("event_time_day"),
    )
)

enriched = (
    valid
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.session_id",
        "src.event_type",
        "src.event_time_day",
        F.col("b.country"),
        F.col("b.device"),
        F.col("b.browser"),
    )
)

latest = (
    enriched
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

scored = (
    latest
    .filter(F.col("session_id").isin([20, 34]))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("seq_no_2", F.row_number().over(w3))
    .withColumn("first_known_device", F.first("device", ignorenulls=True).over(w4))
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w_ordered))
)

result = (
    scored
    .groupBy("device")
    .agg(
        F.count_if(F.col("event_type") == "purchase").alias("purchase_events"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.count(F.when(F.col("browser") == "Chrome", 1)).alias("chrome_count"),
        F.count("*").alias("cnt"),
    )
)
