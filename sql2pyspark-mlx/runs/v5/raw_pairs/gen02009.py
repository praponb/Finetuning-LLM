from pyspark.sql import functions as F

recent_web_events = (
    df_web_events
    .filter((F.col("event_type") == "purchase") & F.col("event_type").isin(["page_view", "search"]))
)

event_quarter_summary = (
    recent_web_events
    .groupBy(F.quarter("event_time").alias("event_quarter"))
    .agg(
        F.count(F.when(F.col("page") == "/cart", 1)).alias("cart_count"),
        F.countDistinct("duration_sec").alias("unique_pages"),
    )
)

result = event_quarter_summary.orderBy(F.col("unique_pages").desc())
