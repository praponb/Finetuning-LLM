from pyspark.sql import functions as F

cleaned = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "event_type",
        F.when(F.col("duration_sec") >= 378, "High").when(F.col("duration_sec") >= 82, "Medium").otherwise("Low").alias("duration_sec_band"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_clean"),
    )
)

remaining_keys = (
    cleaned
    .select("event_type")
    .intersect(
        df_web_events
        .filter(F.col("event_type") == "search")
        .select("event_type")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

cleaned_web_events = (
    reconciled
    .filter(F.col("event_type") == "add_to_cart")
    .select("*", F.initcap(F.trim("event_type")).alias("event_type_title"))
)

filtered = (
    cleaned_web_events
    .filter(F.col("event_type").isin(["page_view", "add_to_cart", "purchase"]))
    .select(
        "*",
        F.when(F.col("event_type") == "purchase", 1).otherwise(0).alias("is_purchase"),
        F.substring("event_type", 1, 4).alias("event_type_prefix"),
    )
)

cleaned_web_events_2 = (
    filtered
    .filter(F.col("is_purchase") > 40)
    .select("*", F.trim("event_type").alias("clean_event_type"))
)

result = (
    cleaned_web_events_2
    .withColumn("is_purchase_tier", F.when(F.col("is_purchase") >= 82, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["is_purchase_tier", "event_type"], ["is_purchase_tier"], []],
        "is_purchase_tier",
        "event_type",
    )
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.count_if(F.col("event_type") == "search").alias("search_events"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list_2"),
    )
    .orderBy(F.col("event_type").desc_nulls_last())
)
