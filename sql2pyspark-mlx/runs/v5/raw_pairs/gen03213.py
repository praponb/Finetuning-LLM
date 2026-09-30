from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("duration_sec_str").desc(), "id")

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-07-29").cast("date")) & F.col("page").isNotNull())
    .withColumn("duration_sec_str", F.col("duration_sec").cast("double"))
    .withColumn("resolved_page", F.coalesce("page", F.lit("N/A")))
)

joined = (
    filtered
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.session_id",
        "src.page",
        "src.duration_sec_str",
        "src.resolved_page",
        F.col("b.browser"),
    )
)

with_dims = (
    joined
    .alias("s")
    .join(df_sessions.alias("p2"), F.col("s.session_id") == F.col("p2.id"), "inner")
    .filter(F.col("p2.customer_id").isin([3, 4, 25, 28]))
    .select("s.*", F.col("p2.device"), F.col("p2.country"))
)

deduped = (
    with_dims
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "session_id", "resolved_page", "browser", "country")
)

result = (
    deduped
    .groupBy("browser")
    .agg(
        F.count(F.when(F.col("country") == "US", 1)).alias("us_rows"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
    )
    .filter(F.col("us_rows") > 1)
    .orderBy(F.col("browser").desc_nulls_last())
)
