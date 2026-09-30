from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") <= 178) & ~F.col("event_type").isin(["page_view", "click"]))
    .select(
        "id",
        "duration_sec",
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
    )
)
