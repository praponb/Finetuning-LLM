from pyspark.sql import functions as F

remaining_keys = (
    df_web_events
    .select("page")
    .exceptAll(
        df_web_events
        .filter(F.col("event_type") == "page_view")
        .select("page")
    )
)

kept_rows = (
    df_web_events
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("page")
    .agg(
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.sum(F.when(F.col("page") == "/product", F.col("duration_sec"))).alias("product_total_duration_sec"),
    )
    .orderBy(F.col("product_total_duration_sec").asc_nulls_first())
)
