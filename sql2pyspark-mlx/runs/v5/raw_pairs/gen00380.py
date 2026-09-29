from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["purchase", "add_to_cart", "page_view"]))
    .groupBy(F.to_date("event_time").alias("event_day"), "event_type")
    .agg(
        F.countDistinct("duration_sec").alias("unique_event_types"),
        F.count(F.when(F.col("page") == "/product", 1)).alias("product_count"),
    )
    .filter(F.col("unique_event_types") < 3)
    .select("event_day", "event_type", "unique_event_types", "product_count")
    .orderBy("unique_event_types")
)
