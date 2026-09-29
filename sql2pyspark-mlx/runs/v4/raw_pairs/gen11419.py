from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter(F.col("event_type").isin(["add_to_cart", "click", "search"]) & F.col("page").isin(["/home", "/product"]) & F.col("session_id").isin([33, 39]))
)

agg = (
    filtered_web_events
    .groupBy("event_type")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0))) / F.count("*")), 1).alias("pct_click"),
        F.round(((100 * F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0))) / F.count("*")), 1).alias("pct_page_view"),
    )
)

result = agg
