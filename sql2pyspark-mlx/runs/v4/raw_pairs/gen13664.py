from pyspark.sql import functions as F

base = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-09-28").cast("date")) & F.col("page").isNotNull())
    .withColumn("page_event_type_key", F.concat_ws("-", "page", "event_type"))
    .withColumn("page_filled", F.coalesce("page", F.lit("N/A")))
)

result = (
    base
    .groupBy(F.date_trunc("year", "event_time").alias("year_start"), "page")
    .agg(
        F.max(F.when(F.col("event_type") == "add_to_cart", F.col("duration_sec"))).alias("add_to_cart_max_duration_sec"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
    )
)
