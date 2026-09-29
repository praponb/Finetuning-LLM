from pyspark.sql import functions as F

kept_keys = (
    df_web_events
    .select("page", "session_id")
    .exceptAll(
        df_web_events
        .filter(~F.col("page").isin(["/cart", "/product", "/home"]))
        .select("page", "session_id")
    )
)

verified = (
    df_web_events
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.page") == F.col("k.page")) & (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(F.date_trunc("QUARTER", "event_time").alias("quarter_start"))
    .agg(
        F.count(F.when(F.col("page") == "/search", 1)).alias("search_rows"),
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_count"),
    )
    .orderBy(F.col("quarter_start").desc_nulls_last())
)
