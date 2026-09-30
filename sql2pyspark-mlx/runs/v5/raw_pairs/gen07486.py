from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("duration_sec", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id")
w3 = Window.orderBy("duration_sec", "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_web_events
    .filter((F.col("event_time") > F.to_timestamp(F.lit("2023-08-29 07:00:00"))) & F.col("event_type").isin(["page_view", "add_to_cart"]))
    .select("*", F.to_date("event_time").alias("event_time_date"))
)

ranked = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

filtered = (
    ranked
    .filter(F.col("event_time") >= F.to_date(F.lit("2023-05-28")))
    .withColumn("is_page_view", F.when(F.col("event_type") == "page_view", 1).otherwise(0))
    .withColumn("page_clean", F.coalesce("page", F.lit("NONE")))
)

joined = (
    filtered
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "left")
    .filter(F.col("d.device") == "tablet")
    .select("s.*", F.col("d.country"))
)

scored = (
    joined
    .withColumn("next_is_page_view", F.lead("is_page_view", 1).over(w2))
    .withColumn("is_page_view_avg_2", F.avg("is_page_view").over(w3))
)

grouped = (
    scored
    .groupBy(
        F.when(F.col("is_page_view") >= 88, "PREMIUM").otherwise("STANDARD").alias("is_page_view_tier"),
    )
    .agg(
        F.min("next_is_page_view").alias("min_next_is_page_view"),
        F.min("event_time").alias("first_seen"),
        F.avg("next_is_page_view").alias("avg_next_is_page_view"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
    )
)

result = (
    grouped
    .select("is_page_view_tier", "first_seen", "jp_count")
    .orderBy(F.col("first_seen").desc_nulls_first())
)
