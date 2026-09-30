from pyspark.sql import functions as F

dim_totals = (
    df_web_events
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "left")
    .rollup(F.col("d.country").alias("country"), F.col("s.page").alias("page"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("s.duration_sec")).alias("max_duration_sec"),
    )
)

result = (
    dim_totals
    .select(
        "country",
        "num_rows",
        "max_duration_sec",
        F.when(F.col("country").rlike("^(UK|DE)$"), "OTHER_ZONE").otherwise("CORE").alias("country_zone"),
    )
)
