from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_time_period").asc_nulls_last(), "id")
w2 = Window.partitionBy("session_id").orderBy("event_time_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy(F.col("event_time_period").desc(), "id")
w4 = Window.orderBy(F.col("duration_sec").asc_nulls_last())
w5 = Window.partitionBy("page")

valid = (
    df_web_events
    .filter(~F.col("event_type").isin(["page_view", "search", "purchase"]))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        F.date_trunc("MONTH", "event_time").alias("event_time_period"),
    )
)

ranked = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

metrics = (
    ranked
    .withColumn("first_known_duration_sec", F.first("duration_sec", ignorenulls=True).over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("duration_sec_cume_dist", F.cume_dist().over(w4))
    .withColumn("page_max_duration_sec", F.max("duration_sec").over(w5))
    .withColumn("running_duration_sec", F.sum("duration_sec").over(w2))
)

daily = (
    metrics
    .groupBy(
        F.when(F.col("row_seq") >= 8, "PREMIUM").otherwise("STANDARD").alias("row_seq_tier"),
        F.date_trunc("quarter", "event_time_period").alias("quarter_start"),
    )
    .agg(
        F.round(F.sum("first_known_duration_sec") / F.nullif(F.count(F.when(F.col("event_type") == "search", 1)), F.lit(0)), 2).alias("first_known_duration_sec_per_search"),
        F.percentile_approx("page_max_duration_sec", [0.25, 0.5, 0.75]).alias("page_max_duration_sec_quartiles"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
    )
)

result = (
    daily
    .select("row_seq_tier", "quarter_start", "event_type_list")
    .orderBy(F.col("row_seq_tier").desc_nulls_last())
)
