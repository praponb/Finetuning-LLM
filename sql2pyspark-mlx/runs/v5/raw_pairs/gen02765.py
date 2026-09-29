from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id")
w2 = Window.partitionBy("session_id").orderBy(F.col("is_cart").desc())
w3 = Window.orderBy(F.col("is_cart").desc_nulls_last())
w4 = Window.orderBy("is_cart_percent_rank")

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-04-18").cast("date")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        F.round("duration_sec", 2).alias("duration_sec_rounded"),
        F.concat_ws("/", "event_type", "page").alias("event_type_page_key"),
    )
)

valid = (
    filtered
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        F.initcap(F.trim("page")).alias("page_title"),
    )
)

valid_web_events = (
    valid
    .filter(F.col("event_type").isin(["page_view", "add_to_cart"]))
    .select(
        "*",
        F.when(F.col("event_type").isin(["page_view", "click"]), "Tier 1").otherwise("Other").alias("event_type_group"),
        F.coalesce("page", F.lit("DIRECT")).alias("resolved_page"),
    )
)

base = (
    valid_web_events
    .filter(F.col("page") == "/cart")
    .select(
        "id",
        "session_id",
        "page",
        "page_title",
        "event_type_group",
        "resolved_page",
        F.when(F.col("page") == "/cart", 1).otherwise(0).alias("is_cart"),
    )
)

remaining_keys = (
    base
    .select("session_id")
    .exceptAll(
        df_web_events
        .filter(F.col("page") != "/cart")
        .select("session_id")
    )
)

verified = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "id",
        "page_title",
        "event_type_group",
        "is_cart",
        F.count("is_cart").over(w).alias("session_id_count_is_cart"),
        F.percent_rank().over(w2).alias("is_cart_percent_rank"),
    )
)

metrics = (
    scored
    .select(
        "page_title",
        "is_cart",
        "session_id_count_is_cart",
        "is_cart_percent_rank",
        F.dense_rank().over(w3).alias("is_cart_dense_rank"),
        F.rank().over(w4).alias("is_cart_percent_rank_rank"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("is_cart_dense_rank") >= 8, "HIGH").otherwise("LOW").alias("is_cart_dense_rank_tier"),
    )
    .agg(
        F.percentile_approx("session_id_count_is_cart", [0.25, 0.5, 0.75]).alias("session_id_count_is_cart_quartiles"),
        F.percentile_approx("is_cart", 0.9).alias("is_cart_p90"),
        F.percentile_approx("is_cart_dense_rank", 0.5).alias("is_cart_dense_rank_p50"),
    )
    .orderBy("is_cart_p90")
)
