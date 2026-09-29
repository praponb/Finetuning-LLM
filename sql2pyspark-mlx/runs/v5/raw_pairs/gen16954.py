from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("device").orderBy("duration_sec", "id")
w_grp = Window.orderBy("country_length", "id")

cleaned = (
    df_web_events
    .filter(F.col("page").isNotNull() & F.col("event_type").isin(["click", "page_view", "search"]))
    .withColumn("duration_sec_str", F.col("duration_sec").cast("double"))
    .withColumn("page_title", F.initcap(F.trim("page")))
    .withColumn("page_filled", F.coalesce("page", F.lit("NONE")))
)

enriched = (
    cleaned
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "inner")
    .filter(F.col("p.country") == "UK")
    .select(
        "src.id",
        "src.session_id",
        "src.page",
        "src.duration_sec",
        "src.page_filled",
        F.col("p.device"),
        F.col("p.country"),
    )
)

filtered = (
    enriched
    .filter(F.col("country") != "JP")
    .select(
        "id",
        "session_id",
        "duration_sec",
        "page_filled",
        "device",
        F.length("country").alias("country_length"),
        F.coalesce("page", F.lit("NONE")).alias("page_filled_2"),
    )
)

with_windows = (
    filtered
    .withColumn("next_country_length", F.lead("country_length").over(w_recent))
    .withColumn("prev_country_length", F.lag("country_length").over(w_recent))
    .withColumn("prev_duration_sec", F.lag("duration_sec").over(w_recent))
    .withColumn("seq_no", F.row_number().over(w_recent))
    .withColumn("next_country_length_2", F.lead("country_length", 1).over(w_grp))
)

result = (
    with_windows
    .select(
        "id",
        "duration_sec",
        "page_filled",
        "device",
        "country_length",
        "page_filled_2",
        "next_country_length",
        "prev_country_length",
        "seq_no",
    )
)
